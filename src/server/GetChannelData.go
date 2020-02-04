package server

import(
	"fmt"
	"pojo"
	"time"
	_"encoding/json"
)

func GetChannelData(){

	for channelName := range TCPStorage {
	    go runChannel(TCPStorage[channelName])
	    time.Sleep(1000)
	}

}

func runChannel(channel *pojo.ChannelStruct){

	defer close(channel.BucketData)

	for{
		
		fmt.Println(<- channel.BucketData)
	}

}