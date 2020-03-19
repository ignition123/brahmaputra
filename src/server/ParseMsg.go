package server

import (
	"net"
	"encoding/json"
	"time"
	"strconv"
	"log"
	"bytes"
	"encoding/binary"
	"io"
	"ChannelList"
	"MongoConnection"
	"pojo"
)


func ParseMsg(msg string, conn net.TCPConn, parseChan chan bool, counterRequest *int){

	defer ChannelList.Recover()

	messageMap := make(map[string]interface{})

	err := json.Unmarshal([]byte(msg), &messageMap)

	if err != nil{
		parseChan <- false
		go ChannelList.WriteLog(err.Error())
		return
	}

	if messageMap["type"] == "heart_beat"{

		if messageMap["channelName"] == ""{
			conn.Close()
			parseChan <- false
			go ChannelList.WriteLog("Invalid message received..." + msg)
			return
		}

		go log.Println("HEART BEAT RECEIVED...")

		messageMap["conn"] = conn
		
		ChannelList.TCPStorage["heart_beat"].BucketData[0] <- messageMap

	}else if messageMap["type"] == "publish"{

		if messageMap["channelName"] == ""{
			conn.Close()
			parseChan <- false
			go ChannelList.WriteLog("Invalid message received..." + msg)
			return
		}

		if messageMap["data"] == ""{
			conn.Close()
			parseChan <- false
			go ChannelList.WriteLog("Data missing..." + msg)
			return
		}

		var channelName = messageMap["channelName"].(string)

		if ChannelList.TCPStorage[channelName] == nil{
			parseChan <- false
			return
		}

		currentTime := time.Now()

		nanoEpoch := currentTime.UnixNano()

		var _id = strconv.FormatInt(nanoEpoch, 10)

		messageMap["_id"] = _id
		
		jsonData, err := json.Marshal(messageMap)

		if err != nil{
			parseChan <- false
			go ChannelList.WriteLog(err.Error())
			return
		}


		if ChannelList.TCPStorage[channelName].ChannelStorageType == "persistent"{

			if *ChannelList.ConfigTCPObj.Storage.File.Active{

				var byteLen = len(jsonData)
			
				go WriteData(jsonData, channelName, byteLen, _id)

				select {

					case message, ok := <-ChannelList.TCPStorage[channelName].WriteCallback:	
						if ok{
							
							if !message{
								parseChan <- false
								return
							}
						}		
				}
			}

			
			if *ChannelList.ConfigTCPObj.Storage.Mongodb.Active{

				var byteLen = len(jsonData)
	 
				go WriteMongodbData(nanoEpoch, messageMap["AgentName"].(string), jsonData, channelName, byteLen)

				select {

					case message, ok := <-ChannelList.TCPStorage[channelName].WriteMongoCallback:	
						if ok{
							
							if !message{
								parseChan <- false
								return
							}
						}		
						break
				}
			} 	
		}

		
		messageMap["conn"] = conn

		if *counterRequest == ChannelList.TCPStorage[channelName].Worker{

			*counterRequest = 0
		}

		ChannelList.TCPStorage[channelName].BucketData[*counterRequest] <- messageMap

		*counterRequest += 1

		parseChan <- true

	}else if messageMap["type"] == "subscribe"{

		if messageMap["channelName"] == ""{
			conn.Close()
			parseChan <- false
			go ChannelList.WriteLog("Invalid message received..." + msg)
			return
		}

		if messageMap["contentMatcher"] == ""{
			conn.Close()
			parseChan <- false
			go ChannelList.WriteLog("Content matcher cannot be empty..." + msg)
			return
		}

		var channelName = messageMap["channelName"].(string)

		if ChannelList.TCPStorage[channelName] == nil{
			parseChan <- false
			return
		}

		log.Println(ChannelList.TCPStorage[channelName].ChannelStorageType)

		if ChannelList.TCPStorage[channelName].ChannelStorageType == "persistent"{

			var ContentMatcher = make(map[string]interface{})

			if messageMap["contentMatcher"] == "all"{

				ContentMatcher = nil

			}else{

				ContentMatcher = messageMap["contentMatcher"].(map[string]interface{})

			}

			now := time.Now()

	    	nanos := now.UnixNano()

	    	var cursor = int64(0)

			go SubscribeChannel(conn, ContentMatcher, nanos, channelName, cursor)

		}else{

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
			
			ChannelList.TCPSocketDetails[channelName] = append(ChannelList.TCPSocketDetails[channelName], socketDetails) 
		}
		
		parseChan <- true 

	}else{
		conn.Close()
		parseChan <- false
		go ChannelList.WriteLog("Invalid message type must be either publish or subscribe...")
		return
	}	
}

func WriteMongodbData(_id int64, agentName string, jsonData []byte, channelName string, byteLen int){

	defer ChannelList.Recover()

	var packetBuffer bytes.Buffer

	buff := make([]byte, 4)

	binary.LittleEndian.PutUint32(buff, uint32(byteLen))

	packetBuffer.Write(buff)

	packetBuffer.Write(jsonData)

	var oneDoc = make(map[string]interface{})

	oneDoc["offsetID"] = _id
	oneDoc["AgentName"] = agentName
	oneDoc["cluster"] = *ChannelList.ConfigTCPObj.Server.TCP.ClusterName
	oneDoc["data"] = packetBuffer.Bytes()

	var status, _ = MongoConnection.InsertOne(channelName, oneDoc)

	if !status{
		ChannelList.TCPStorage[channelName].WriteMongoCallback <- false
	}else{
		ChannelList.TCPStorage[channelName].WriteMongoCallback <- true
	}
}

func WriteData(jsonData []byte, channelName string, byteLen int, _id string){

	defer ChannelList.Recover()

	ChannelList.TCPStorage[channelName].ChannelLock.Lock()

	var packetBuffer bytes.Buffer

	buff := make([]byte, 4)

	binary.LittleEndian.PutUint32(buff, uint32(byteLen))

	packetBuffer.Write(buff)

	packetBuffer.Write(jsonData)
 
	_, err := ChannelList.TCPStorage[channelName].FD.Write(packetBuffer.Bytes())

	ChannelList.TCPStorage[channelName].ChannelLock.Unlock()
	
	if (err != nil && err != io.EOF ){
		go ChannelList.WriteLog(err.Error())
		ChannelList.TCPStorage[channelName].WriteCallback <- false
		return
	}

	ChannelList.TCPStorage[channelName].WriteCallback <- true
}
