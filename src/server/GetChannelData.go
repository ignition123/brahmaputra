package server

import(
	"fmt"
	"pojo"
	"time"
	_"encoding/json"
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

		<- channel.BucketData

		// var channelName = message["channelName"].(string)

		fmt.Println(TCPSocketDetails[channelName][0])
		
		// fmt.Println(message)
	}

}