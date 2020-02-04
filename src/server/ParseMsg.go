package server

import (
	_"pojo"
	"net"
	"encoding/json"
	"fmt"
	"sync"
	_"time"
)

var mutex = &sync.Mutex{}

func ParseMsg(msg string, conn net.Conn){

	messageMap := make(map[string]interface{})

	err := json.Unmarshal([]byte(msg), &messageMap)

	if err != nil{
		fmt.Println(err.Error())
		WriteLog(err.Error())
		return
	}

	if messageMap["type"] == "publish"{

		if messageMap["_id"] == 0{
			fmt.Println("Invalid message received..." + msg)
			WriteLog("Invalid message received..." + msg)
			return
		}

		if messageMap["channelName"] == ""{
			fmt.Println("Invalid message received..." + msg)
			WriteLog("Invalid message received..." + msg)
			return
		}

		var channelName = messageMap["channelName"].(string)

		mutex.Lock()

		TCPStorage[channelName].BucketData <- messageMap["data"].(map[string]interface{})

		mutex.Unlock()

	}else if messageMap["type"] == "subscribe"{

		

	}else{
		fmt.Println("Invalid message type must be either publish or subscribe...")
		WriteLog("Invalid message type must be either publish or subscribe...")
		return
	}	
}