package server

import (
	"pojo"
	"net"
	"encoding/json"
	"time"
	"strconv"
	"fmt"
	"bytes"
	"encoding/binary"
	"io"
	"sync"
)

var writeDataCallback = make(chan bool)
var writeCallback = make(chan bool)

var AckMutex = &sync.Mutex{}
var FileWriteLock = &sync.Mutex{}
var FileTableLock = &sync.Mutex{}

func ParseMsg(msg string, conn net.Conn){

	messageMap := make(map[string]interface{})

	err := json.Unmarshal([]byte(msg), &messageMap)

	if err != nil{
		go WriteLog(err.Error())
		return
	}

	if messageMap["type"] == "heart_beat"{

		if messageMap["channelName"] == ""{
			conn.Close()
			go WriteLog("Invalid message received..." + msg)
			return
		}

		fmt.Println("HEART BEAT RECEIVED...")

		TCPStorage["heart_beat"].BucketData[0] <- messageMap

		conn.Write([]byte(msg))

	}else if messageMap["type"] == "publish"{

		if messageMap["channelName"] == ""{
			conn.Close()
			go WriteLog("Invalid message received..." + msg)
			return
		}

		if messageMap["data"] == ""{
			conn.Close()
			go WriteLog("Data missing..." + msg)
			return
		}

		var channelName = messageMap["channelName"].(string)

		currentTime := time.Now()

		epoch := currentTime.Unix()

		var _id = strconv.FormatInt(currentTime.UnixNano(), 10)

		messageMap["_id"] = _id

		var indexNo = epoch % int64(TCPStorage[channelName].Worker)
		
		jsonData, err := json.Marshal(messageMap)

		if err != nil{
			go WriteLog(err.Error())
			return
		}

		if *ConfigTCPObj.Storage.File.Active{
			var byteLen = len(jsonData)
		
			WRITEFILE: 
				go WriteData(jsonData, channelName, byteLen, writeDataCallback)

				if !<- writeDataCallback{
					goto WRITEFILE
				}

			WRITEOFFSET: 
				go WriteOffset(jsonData, channelName, byteLen, _id, writeCallback)

				if !<- writeCallback{
					goto WRITEOFFSET
				}
		}

		go SendAck(messageMap, conn)

		TCPStorage[channelName].BucketData[indexNo] <- messageMap

	}else if messageMap["type"] == "subscribe"{

		if messageMap["channelName"] == ""{
			conn.Close()
			go WriteLog("Invalid message received..." + msg)
			return
		}

		if messageMap["contentMatcher"] == ""{
			conn.Close()
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
		conn.Close()
		go WriteLog("Invalid message type must be either publish or subscribe...")
		return
	}	
}

// func ReadDataUsingOffset(){
// 	var byteSize = (byteLen + 4)

// 	var readByte = make([]byte, byteSize)

// 	_, err = TCPStorage[channelName].FD.ReadAt(readByte, TCPStorage[channelName].Offset)

// 	if err != nil{
// 		fmt.Println(err.Error())
// 		return
// 	}

// 	TCPStorage[channelName].Offset += int64(byteSize)

// 	readByte = readByte[4:]

// 	messageMap = make(map[string]interface{})

// 	err = json.Unmarshal(readByte, &messageMap)

// 	if err != nil{
// 		go WriteLog(err.Error())
// 		return
// 	}
// }

func SendAck(messageMap map[string]interface{}, conn net.Conn){

	AckMutex.Lock()
	defer AckMutex.Unlock()

	if(messageMap["type"] == "publish"){

		var messageResp = make(map[string]interface{})

		messageResp["producer_id"] = messageMap["producer_id"].(string)

		jsonData, err := json.Marshal(messageResp)

		if err != nil{
			go WriteLog(err.Error())
			return
		}

		var packetBuffer bytes.Buffer

		buff := make([]byte, 4)

		binary.LittleEndian.PutUint32(buff, uint32(len(jsonData)))

		packetBuffer.Write(buff)

		packetBuffer.Write(jsonData)

		RETRY: _, err = conn.Write(packetBuffer.Bytes())

		if err != nil{

			time.Sleep(2 * time.Second)

			goto RETRY

		}
	}	
}

func WriteData(jsonData []byte, channelName string, byteLen int, writeDataCallback chan bool){

	FileWriteLock.Lock()
	defer FileWriteLock.Unlock()

	var packetBuffer bytes.Buffer

	buff := make([]byte, 4)

	binary.LittleEndian.PutUint32(buff, uint32(byteLen))

	packetBuffer.Write(buff)

	packetBuffer.Write(jsonData)

	if _, err := TCPStorage[channelName].FD.Write(packetBuffer.Bytes()); err != nil && err != io.EOF {
		go WriteLog(err.Error())
		writeDataCallback <- false
	}

	writeDataCallback <- true
}

func WriteOffset(jsonData []byte, channelName string, byteLen int, _id string, writeCallback chan bool){

	FileTableLock.Lock()
	defer FileTableLock.Unlock()

	var byteSize = (byteLen + 4)

	TCPStorage[channelName].Offset += int64(byteSize)

	var offset = strconv.FormatInt(TCPStorage[channelName].Offset, 10)

	var offsetString = *ConfigTCPObj.Server.TCP.ClusterName+"|"+offset+"|"+_id+"\r\n"

	var packetBuffer bytes.Buffer

	buff := make([]byte, 2)

	binary.LittleEndian.PutUint16(buff, uint16(len(offsetString)))

	packetBuffer.Write(buff)
	packetBuffer.Write([]byte(offsetString))

	if _, err := TCPStorage[channelName].TableFD.Write(packetBuffer.Bytes()); err != nil && err != io.EOF {
		go WriteLog(err.Error())
		writeCallback <- false
	}

	writeCallback <- true
}