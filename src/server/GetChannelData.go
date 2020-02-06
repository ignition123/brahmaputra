package server

import(
	"pojo"
	"encoding/json"
	"bytes"
	"encoding/binary"
	"context"
)

func GetChannelData(){

	for channelName := range TCPStorage {
	    go runChannel(channelName)
	}

}

func runChannel(channelName string){

	defer close(TCPStorage[channelName].BucketData)

	ctx := context.Background()

	for{

		select {
			case message, ok := <- TCPStorage[channelName].BucketData:
				if ok{
					var subchannelName = message["channelName"].(string)

					if(channelName == subchannelName && len(TCPSocketDetails[channelName]) > 0){					
						sendMessageToClient(message, TCPSocketDetails, channelName)
					}
				}
			case <-ctx.Done():
				go WriteLog("Channel closed...")
				break	
			default:
				//fmt.Println("Waiting for messagses...")
		}		
	}

}

func sendMessageToClient(message map[string]interface{}, TCPSocketDetails map[string][]*pojo.SocketDetails, channelName string){

	for index := range TCPSocketDetails[channelName]{

		var packetBuffer bytes.Buffer

		sizeBuff := make([]byte, 4)

		if len(TCPSocketDetails[channelName]) <= index{
			break
		} 

		if TCPSocketDetails[channelName][index].ContentMatcher == nil{

			jsonData, err := json.Marshal(message)

			if err != nil{
				go WriteLog(err.Error())
				break
			}

			binary.LittleEndian.PutUint32(sizeBuff, uint32(len(jsonData)))
			packetBuffer.Write(sizeBuff)
			packetBuffer.Write(jsonData)

			go send(TCPSocketDetails, channelName, index, packetBuffer)

		}else{

			var cm = TCPSocketDetails[channelName][index].ContentMatcher

			var matchFound = true

			var messageData = message["data"].(map[string]interface{})

			for key := range cm{

				if cm[key] != messageData[key]{
					matchFound = false
					break
				}

			}

			if matchFound == true{

				jsonData, err := json.Marshal(message)

				if err != nil{
					go WriteLog(err.Error())
					break
				}

				binary.LittleEndian.PutUint32(sizeBuff, uint32(len(jsonData)))
				packetBuffer.Write(sizeBuff)
				packetBuffer.Write(jsonData)

				go send(TCPSocketDetails, channelName, index, packetBuffer)

			}

		}

	}
}

func send(TCPSocketDetails map[string][]*pojo.SocketDetails, channelName string, index int, packetBuffer bytes.Buffer){

	if len(TCPSocketDetails[channelName]) <= index{
		return
	} 

	_, err := TCPSocketDetails[channelName][index].Conn.Write(packetBuffer.Bytes())

	if err != nil {
	
		go WriteLog(err.Error())

		var channelArray = TCPSocketDetails[channelName]

		if len(channelArray) <= index{
			return
		}
	
		copy(channelArray[index:], channelArray[index+1:])
		channelArray[len(channelArray)-1] = nil
		TCPSocketDetails[channelName] = channelArray[:len(channelArray)-1]

	}

}