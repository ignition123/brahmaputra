package server

import(
	"encoding/json"
	"bytes"
	"encoding/binary"
	"sync"
	_"log"
	"ChannelList"
	"time"
	"net"
	"os"
)

type ChannelMethods struct{
	sync.RWMutex
}

func (e *ChannelMethods) GetChannelData(){

	defer ChannelList.Recover()

	for channelName := range ChannelList.TCPStorage {

	    e.runChannel(channelName)

	}

}

func (e *ChannelMethods) runChannel(channelName string){

	defer ChannelList.Recover()

	for index := range ChannelList.TCPStorage[channelName].BucketData{

		time.Sleep(100)

		go func(index int, BucketData chan map[string]interface{}, channelName string){

			defer ChannelList.Recover()

			defer close(BucketData)

			var cbChan = make(chan bool, 1)

			for{

				select {

					case message, ok := <-BucketData:	

						if ok{

							var subchannelName = message["channelName"].(string)

							if(channelName == subchannelName && channelName != "heart_beat"){

								var conn = message["conn"].(net.TCPConn)

								delete(message, "conn")	

								if ChannelList.TCPStorage[channelName].ChannelStorageType == "persistent"{

									go e.SendAck(message, conn, cbChan)

									<-cbChan

								}else{

									go e.sendMessageToClient(conn, message, channelName, cbChan)

									<-cbChan

								}
							}
						}		
					break
				}		
			}

		}(index, ChannelList.TCPStorage[channelName].BucketData[index], channelName)
	}
}

func (e *ChannelMethods) sendMessageToClient(conn net.TCPConn, message map[string]interface{}, channelName string, cbChan chan bool){

	defer ChannelList.Recover()

	ackChan := make(chan bool, 1)
	subsChan := make(chan bool, 1)

	go e.SendAck(message, conn, ackChan)

	select {

		case _, ok := <-ackChan:	

			if ok{

			}
		break
	}

	for index := range ChannelList.TCPSocketDetails[channelName]{

		var packetBuffer bytes.Buffer

		if len(ChannelList.TCPSocketDetails[channelName]) <= index{
			break
		} 

		var cm = ChannelList.TCPSocketDetails[channelName][index].ContentMatcher

		var matchFound = true

		var messageData = message["data"].(map[string]interface{})


		if _, found := cm["$and"]; found {
		    
		    matchFound = AndMatch(messageData, cm)

		}else if _, found := cm["$or"]; found {

			matchFound = OrMatch(messageData, cm)

		}else if _, found := cm["$eq"]; found {

			if cm["$eq"] == "all"{

				matchFound = true

			}else{

				matchFound = false

			}

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

			go e.sendInMemory(channelName, index, packetBuffer, subsChan)

			select {

				case _, ok := <-subsChan:	

					if ok{

					}
				break
			}
		}

	}

	cbChan <- true
}

func (e *ChannelMethods) sendInMemory(channelName string, index int, packetBuffer bytes.Buffer, callback chan bool){ 

	defer ChannelList.Recover()

	var totalRetry = 0

	RETRY:

	if len(ChannelList.TCPSocketDetails[channelName]) > index{

		totalRetry += 1

		if totalRetry > 5{

			callback <- false

			return

		}

		_, err := ChannelList.TCPSocketDetails[channelName][index].Conn.Write(packetBuffer.Bytes())
		
		if err != nil {
		
			go ChannelList.WriteLog(err.Error())

			var channelArray = ChannelList.TCPSocketDetails[channelName]
			copy(channelArray[index:], channelArray[index+1:])
			channelArray[len(channelArray)-1] = nil
			ChannelList.TCPSocketDetails[channelName] = channelArray[:len(channelArray)-1]

			goto RETRY

		}

		callback <- true

	}else{

		callback <- false

	}

}

func SubscribeChannel(conn net.TCPConn, cm map[string]interface{}, lastTime int64, channelName string, cursor int64){

	file, err := os.Open(ChannelList.TCPStorage[channelName].Path)

	defer file.Close()

	if err != nil {
		go ChannelList.WriteLog(err.Error())
		return
	}

	var sentMsg = make(chan bool)

	var exitLoop = false

	RETRY:

	for{

		if exitLoop{

			break
		}

		fileStat, err := os.Stat(ChannelList.TCPStorage[channelName].Path)
 
		if err != nil {
			go ChannelList.WriteLog(err.Error())
			break
		}

		if cursor < fileStat.Size(){

			data := make([]byte, 4)

			count, err := file.ReadAt(data, cursor)

			if err != nil {

				continue

			}

			if count > 0{

				cursor += 4

				var packetSize = binary.LittleEndian.Uint32(data)

				restPacket := make([]byte, packetSize)

				packetCount, errPacket := file.ReadAt(restPacket, cursor)

				if errPacket != nil{

					continue

				}

				if packetCount > 0{

					cursor += int64(packetSize)

					message := make(map[string]interface{})

					errJson := json.Unmarshal(restPacket, &message)

					if errJson != nil{
						
						continue

					}

					var matchFound = true

					var messageData = message["data"].(map[string]interface{})


					if _, found := cm["$and"]; found {
					    
					    matchFound = AndMatch(messageData, cm)

					}else if _, found := cm["$or"]; found {

						matchFound = OrMatch(messageData, cm)

					}else if _, found := cm["$eq"]; found {

						if cm["$eq"] == "all"{

							matchFound = true

						}else{

							matchFound = false

						}

					}else{

						matchFound = false

					}

					if matchFound == true{

						sizeBuff := make([]byte, 4)

						var packetBuffer bytes.Buffer

						binary.LittleEndian.PutUint32(sizeBuff, uint32(len(restPacket)))
						packetBuffer.Write(sizeBuff)
						packetBuffer.Write(restPacket)

						go send(conn, channelName, packetBuffer, sentMsg)

						select{
							case message, ok := <-sentMsg:	
								if ok{

									if !message{
										exitLoop = true
									}else{
										exitLoop = false
									}

								}

								break
						}
					}

				}else{

					time.Sleep(1 * time.Second)

				}

			}else{

				time.Sleep(1 * time.Second)
			}

		}else{

			time.Sleep(1 * time.Second)
		}
	}
		

	time.Sleep(5 * time.Second)

	goto RETRY

}

func send(conn net.TCPConn, channelName string, packetBuffer bytes.Buffer, sentMsg chan bool){ 

	defer ChannelList.Recover()

	var totalRetry = 0

	RETRY:

	totalRetry += 1

	if totalRetry > 5{

		sentMsg <- false

		return

	}

	_, err := conn.Write(packetBuffer.Bytes())
	
	if err != nil {
	
		go ChannelList.WriteLog(err.Error())

		time.Sleep(1 * time.Second)

		goto RETRY

	}

	sentMsg <- true

}

func (e *ChannelMethods) SendAck(messageMap map[string]interface{}, conn net.TCPConn, ackChan chan bool){

	defer ChannelList.Recover()

	var messageResp = make(map[string]interface{})

	messageResp["producer_id"] = messageMap["producer_id"].(string)

	jsonData, err := json.Marshal(messageResp)

	if err != nil{

		go ChannelList.WriteLog(err.Error())

		ackChan <- false

		return
	}

	var packetBuffer bytes.Buffer

	buff := make([]byte, 4)

	binary.LittleEndian.PutUint32(buff, uint32(len(jsonData)))

	packetBuffer.Write(buff)

	packetBuffer.Write(jsonData)

	_, err = conn.Write(packetBuffer.Bytes())

	if err != nil{

		go ChannelList.WriteLog(err.Error())

		ackChan <- false

		return
	}

	ackChan <- true

}