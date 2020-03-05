package server

import(
	_"pojo"
	"encoding/json"
	"bytes"
	"encoding/binary"
	_"sync"
	_"fmt"
	"ChannelList"
	"time"
	"net"
)

func GetChannelData(){

	defer ChannelList.Recover()

	for channelName := range ChannelList.TCPStorage {

		var messageChan = make(chan bool)

	    runChannel(channelName, messageChan)

	}

}

func runChannel(channelName string, messageChan chan bool){

	defer ChannelList.Recover()

	for index := range ChannelList.TCPStorage[channelName].BucketData{

		time.Sleep(100)

		go func(BucketData chan map[string]interface{}, channelName string){

			defer ChannelList.Recover()

			defer close(BucketData)

			for{

				select {

					case message, ok := <-BucketData:	

						if ok{

							var subchannelName = message["channelName"].(string)

							if(channelName == subchannelName && channelName != "heart_beat"){	

								go sendMessageToClient(message, channelName, messageChan)
							}
						}		
						break
					default:
						<-time.After(1 * time.Nanosecond)
						break
				}		
			}

		}(ChannelList.TCPStorage[channelName].BucketData[index], channelName)
	}
}

func sendMessageToClient(message map[string]interface{}, channelName string, messageChan chan bool){

	defer ChannelList.Recover()

	var subscriberSentCount = 0

	var conn = message["conn"].(net.TCPConn)

	delete(message, "conn")

	for index := range ChannelList.TCPSocketDetails[channelName]{

		var packetBuffer bytes.Buffer

		if len(ChannelList.TCPSocketDetails[channelName]) <= index{
			break
		} 

		if ChannelList.TCPSocketDetails[channelName][index].ContentMatcher == nil{

			jsonData, err := json.Marshal(message)

			if err != nil{
				go ChannelList.WriteLog(err.Error())
				break
			}

			sizeBuff := make([]byte, 4)

			binary.LittleEndian.PutUint32(sizeBuff, uint32(len(jsonData)))
			packetBuffer.Write(sizeBuff)
			packetBuffer.Write(jsonData)

			go send(channelName, index, packetBuffer, messageChan)

		}else{

			var cm = ChannelList.TCPSocketDetails[channelName][index].ContentMatcher

			var matchFound = true

			var messageData = message["data"].(map[string]interface{})


			if _, found := cm["$and"]; found {
			    
			    matchFound = AndMatch(messageData, cm)

			}else if _, found := cm["$or"]; found {

				matchFound = OrMatch(messageData, cm)

			}else{

				matchFound = false

			}

			if matchFound == true{

				jsonData, err := json.Marshal(message)

				if err != nil{
					go ChannelList.WriteLog(err.Error())
					break
				}

				sizeBuff := make([]byte, 4)

				binary.LittleEndian.PutUint32(sizeBuff, uint32(len(jsonData)))
				packetBuffer.Write(sizeBuff)
				packetBuffer.Write(jsonData)

				go send(channelName, index, packetBuffer, messageChan)

			}

		}

		select {

			case chanCallback, ok := <-messageChan:	
				if ok{
					
					if chanCallback{

						subscriberSentCount += 1

					}
				}	
				break
			default:
				<-time.After(1 * time.Nanosecond)
				break
		}

	}

	if len(ChannelList.TCPSocketDetails[channelName]) == 0{

		go SendAck(message, conn, messageChan)

	}else{

		if subscriberSentCount == len(ChannelList.TCPSocketDetails[channelName]){

			go SendAck(message, conn, messageChan)

		}

	}

	<-messageChan
}

func send(channelName string, index int, packetBuffer bytes.Buffer, messageChan chan bool){

	defer ChannelList.Recover()
		
	if len(ChannelList.TCPSocketDetails[channelName]) > index{

		_, err := ChannelList.TCPSocketDetails[channelName][index].Conn.Write(packetBuffer.Bytes())

		if err != nil {
		
			go ChannelList.WriteLog(err.Error())

			var channelArray = ChannelList.TCPSocketDetails[channelName]
		
			copy(channelArray[index:], channelArray[index+1:])
			channelArray[len(channelArray)-1] = nil
			ChannelList.TCPSocketDetails[channelName] = channelArray[:len(channelArray)-1]

		}

	}

	messageChan <- true
}

func SendAck(messageMap map[string]interface{}, conn net.TCPConn, messageChan chan bool){

	defer ChannelList.Recover()

	var messageResp = make(map[string]interface{})

	messageResp["producer_id"] = messageMap["producer_id"].(string)

	jsonData, err := json.Marshal(messageResp)

	if err != nil{

		messageChan <- false

		go ChannelList.WriteLog(err.Error())

		return
	}

	var packetBuffer bytes.Buffer

	buff := make([]byte, 4)

	binary.LittleEndian.PutUint32(buff, uint32(len(jsonData)))

	packetBuffer.Write(buff)

	packetBuffer.Write(jsonData)

	var counter = 0

	RETRY: _, err = conn.Write(packetBuffer.Bytes())

	if err != nil && counter <= 5{

		time.Sleep(2 * time.Second)

		counter += 1

		goto RETRY

	}

	messageChan <- true
}