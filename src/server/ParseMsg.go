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

	mutex.Lock()

	messageMap := make(map[string]interface{})

	err := json.Unmarshal([]byte(msg), &messageMap)

	if err != nil{
		fmt.Println(err.Error())
		WriteLog(err.Error())
		return
	}
	
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

	var channelMess = TCPStorage[channelName]["bucketData"].(chan string)

	channelMess <- messageMap["data"].(string)

	mutex.Unlock()
}