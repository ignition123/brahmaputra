package server

import (
	"pojo"
	"net"
	"encoding/json"
	"fmt"
	"sync"
	"time"
	"strconv"
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

		if messageMap["channelName"] == ""{
			fmt.Println("Invalid message received..." + msg)
			WriteLog("Invalid message received..." + msg)
			return
		}

		if messageMap["data"] == ""{
			fmt.Println("Data missing..." + msg)
			WriteLog("Data missing..." + msg)
			return
		}

		var channelName = messageMap["channelName"].(string)

		mutex.Lock()

		currentTime := time.Now()

		var _id = strconv.FormatInt(currentTime.UnixNano(), 10)

		messageMap["_id"] = _id
		
		TCPStorage[channelName].BucketData <- messageMap

		mutex.Unlock()

	}else if messageMap["type"] == "subscribe"{

		if messageMap["channelName"] == ""{
			fmt.Println("Invalid message received..." + msg)
			WriteLog("Invalid message received..." + msg)
			return
		}

		if messageMap["contentMatcher"] == ""{
			fmt.Println("Content matcher cannot be empty..." + msg)
			WriteLog("Content matcher cannot be empty..." + msg)
			return
		}

		var channelName = messageMap["channelName"].(string)

		mutex.Lock()

		var socketDetails *pojo.SocketDetails

		if messageMap["contentMatcher"] == "all"{

			socketDetails = &pojo.SocketDetails{
				Conn:conn,
				ContentMatcher: nil,
			}

		}else{
			socketDetails = &pojo.SocketDetails{
				Conn:conn,
				ContentMatcher: messageMap["contentMatcher"].(map[string]interface{}),
			}
		}
		

		TCPSocketDetails[channelName] = append(TCPSocketDetails[channelName], socketDetails)  

		mutex.Unlock()

	}else{
		fmt.Println("Invalid message type must be either publish or subscribe...")
		WriteLog("Invalid message type must be either publish or subscribe...")
		return
	}	
}