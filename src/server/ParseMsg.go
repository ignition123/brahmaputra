package server

import (
	"pojo"
	"net"
	"encoding/json"
	"time"
	"strconv"
	"sync"
	"fmt"
)

func ParseMsg(msg string, mutex *sync.Mutex, conn net.Conn){

	messageMap := make(map[string]interface{})

	err := json.Unmarshal([]byte(msg), &messageMap)

	if err != nil{
		go WriteLog(err.Error())
		return
	}

	if messageMap["type"] == "heart_beat"{

		fmt.Println("HEART BEAT RECEIVED...")
		return

	}

	if messageMap["type"] == "publish"{

		if messageMap["channelName"] == ""{
			go WriteLog("Invalid message received..." + msg)
			return
		}

		if messageMap["data"] == ""{
			go WriteLog("Data missing..." + msg)
			return
		}

		var channelName = messageMap["channelName"].(string)

		currentTime := time.Now()

		epoch := currentTime.Unix()

		var _id = strconv.FormatInt(currentTime.UnixNano(), 10)

		messageMap["_id"] = _id
		
		var indexNo = epoch % int64(TCPStorage[channelName].Worker)
		
		TCPStorage[channelName].BucketData[indexNo] <- messageMap

	}else if messageMap["type"] == "subscribe"{

		if messageMap["channelName"] == ""{
			go WriteLog("Invalid message received..." + msg)
			return
		}

		if messageMap["contentMatcher"] == ""{
			go WriteLog("Content matcher cannot be empty..." + msg)
			return
		}

		var channelName = messageMap["channelName"].(string)

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

	}else{
		go WriteLog("Invalid message type must be either publish or subscribe...")
		return
	}	

	mutex.Unlock()
}