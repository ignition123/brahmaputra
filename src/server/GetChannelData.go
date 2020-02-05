package server

import(
	"fmt"
	"pojo"
	"time"
	"encoding/json"
	"bytes"
	"encoding/binary"
	_"bufio"
)

func GetChannelData(){

	for channelName := range TCPStorage {
	    go runChannel(TCPStorage[channelName], channelName)
	    time.Sleep(1000)
	}

}

func runChannel(channel *pojo.ChannelStruct, channelName string){

	defer close(channel.BucketData)

	for{

		var message = <- channel.BucketData

		if(len(TCPSocketDetails[channelName]) > 0){
			
			sendMessageToClient(message, TCPSocketDetails, channelName)

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
				fmt.Println(err.Error())
				WriteLog(err.Error())
				break
			}

			binary.LittleEndian.PutUint32(sizeBuff, uint32(len(jsonData)))
			packetBuffer.Write(sizeBuff)
			packetBuffer.Write(jsonData)

			go send(TCPSocketDetails, channelName, index, packetBuffer)

		}else{

			var cm = TCPSocketDetails[channelName][index].ContentMatcher

			var matchFound = true

			for key := range cm{

				if cm[key] != message[key]{
					matchFound = false
					break
				}

			}

			if matchFound == true{

				jsonData, err := json.Marshal(message)

				if err != nil{
					fmt.Println(err.Error())
					WriteLog(err.Error())
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

	fmt.Println(time.Now())

	_, err := TCPSocketDetails[channelName][index].Conn.Write(packetBuffer.Bytes())

	if err != nil {
		
		time.Sleep(5000)

		fmt.Println(err.Error())
		WriteLog(err.Error())

		var channelArray = TCPSocketDetails[channelName]

		if len(channelArray) <= index{
			return
		}
	
		copy(channelArray[index:], channelArray[index+1:])
		channelArray[len(channelArray)-1] = nil
		TCPSocketDetails[channelName] = channelArray[:len(channelArray)-1]

	}

}