package server

import(
	_"pojo"
	"encoding/json"
	"bytes"
	"encoding/binary"
	"context"
	"sync"
	_"fmt"
	"ChannelList"
)

var channelMutex = &sync.Mutex{}

var parseMessageMutex = &sync.Mutex{}

func GetChannelData(){

	for channelName := range ChannelList.TCPStorage {
	    runChannel(channelName)
	}

}

func runChannel(channelName string){

	for index := range ChannelList.TCPStorage[channelName].BucketData{
		go func(BucketData chan map[string]interface{}, channelName string){

			defer close(BucketData)

			ctx := context.Background()

			for{

				select {

					case message, ok := <-BucketData:	

					if ok{
						var subchannelName = message["channelName"].(string)

						if(channelName == subchannelName && len(ChannelList.TCPSocketDetails[channelName]) > 0){	
							go sendMessageToClient(message, channelName)
						}
					}		

					break
					case <-ctx.Done():
						go ChannelList.WriteLog("Channel closed...")
					break
				}		
			}

		}(ChannelList.TCPStorage[channelName].BucketData[index], channelName)
	}
}

func sendMessageToClient(message map[string]interface{}, channelName string){

	for index := range ChannelList.TCPSocketDetails[channelName]{

		parseMessageMutex.Lock()

		defer parseMessageMutex.Unlock()

		var packetBuffer bytes.Buffer

		sizeBuff := make([]byte, 4)

		if len(ChannelList.TCPSocketDetails[channelName]) <= index{
			break
		} 

		if ChannelList.TCPSocketDetails[channelName][index].ContentMatcher == nil{

			jsonData, err := json.Marshal(message)

			if err != nil{
				go ChannelList.WriteLog(err.Error())
				break
			}

			binary.LittleEndian.PutUint32(sizeBuff, uint32(len(jsonData)))
			packetBuffer.Write(sizeBuff)
			packetBuffer.Write(jsonData)

			go send(channelName, index, packetBuffer)

		}else{

			var cm = ChannelList.TCPSocketDetails[channelName][index].ContentMatcher

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
					go ChannelList.WriteLog(err.Error())
					break
				}

				binary.LittleEndian.PutUint32(sizeBuff, uint32(len(jsonData)))
				packetBuffer.Write(sizeBuff)
				packetBuffer.Write(jsonData)

				go send(channelName, index, packetBuffer)

			}

		}

	}
}

func send(channelName string, index int, packetBuffer bytes.Buffer){

	channelMutex.Lock()
	
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

	channelMutex.Unlock()
}