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
	"ChannelList"
	"MongoConnection"
)

var writeDataCallback = make(chan bool)
var writeCallback = make(chan bool)

var AckMutex = &sync.Mutex{}
var FileWriteLock = &sync.Mutex{}
var FileTableLock = &sync.Mutex{}
var MongoDBLock = &sync.Mutex{}

func ParseMsg(msg string, conn net.Conn){

	defer ChannelList.Handlepanic()

	messageMap := make(map[string]interface{})

	err := json.Unmarshal([]byte(msg), &messageMap)

	if err != nil{
		go ChannelList.WriteLog(err.Error())
		return
	}

	if messageMap["type"] == "heart_beat"{

		if messageMap["channelName"] == ""{
			conn.Close()
			go ChannelList.WriteLog("Invalid message received..." + msg)
			return
		}

		fmt.Println("HEART BEAT RECEIVED...")

		ChannelList.TCPStorage["heart_beat"].BucketData[0] <- messageMap

		conn.Write([]byte(msg))

	}else if messageMap["type"] == "publish"{

		if messageMap["channelName"] == ""{
			conn.Close()
			go ChannelList.WriteLog("Invalid message received..." + msg)
			return
		}

		if messageMap["data"] == ""{
			conn.Close()
			go ChannelList.WriteLog("Data missing..." + msg)
			return
		}

		var channelName = messageMap["channelName"].(string)

		currentTime := time.Now()

		epoch := currentTime.Unix()

		nanoEpoch := currentTime.UnixNano()

		var _id = strconv.FormatInt(nanoEpoch, 10)

		messageMap["_id"] = _id

		var indexNo = epoch % int64(ChannelList.TCPStorage[channelName].Worker)
		
		jsonData, err := json.Marshal(messageMap)

		if err != nil{
			go ChannelList.WriteLog(err.Error())
			return
		}

		if *ChannelList.ConfigTCPObj.Storage.File.Active{

			var byteLen = len(jsonData)
		
			go WriteData(jsonData, channelName, byteLen, writeDataCallback)

			if !<- writeDataCallback{
				return
			}

			go WriteOffset(jsonData, channelName, byteLen, _id, writeCallback)

			if !<- writeCallback{
				return
			}
		}

		if *ChannelList.ConfigTCPObj.Storage.Mongodb.Active{

			var byteLen = len(jsonData)
 
			go WriteMongodbData(nanoEpoch, jsonData, channelName, byteLen, writeDataCallback)

			if !<- writeDataCallback{
				return
			}
		} 

		go SendAck(messageMap, conn)

		ChannelList.TCPStorage[channelName].BucketData[indexNo] <- messageMap

	}else if messageMap["type"] == "subscribe"{

		if messageMap["channelName"] == ""{
			conn.Close()
			go ChannelList.WriteLog("Invalid message received..." + msg)
			return
		}

		if messageMap["contentMatcher"] == ""{
			conn.Close()
			go ChannelList.WriteLog("Content matcher cannot be empty..." + msg)
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
		
		ChannelList.TCPSocketDetails[channelName] = append(ChannelList.TCPSocketDetails[channelName], socketDetails)  

	}else{
		conn.Close()
		go ChannelList.WriteLog("Invalid message type must be either publish or subscribe...")
		return
	}	
}

// func ReadDataUsingOffset(){
// 	var byteSize = (byteLen + 4)

// 	var readByte = make([]byte, byteSize)

// 	_, err = ChannelList.TCPStorage[channelName].FD.ReadAt(readByte, ChannelList.TCPStorage[channelName].Offset)

// 	if err != nil{
// 		fmt.Println(err.Error())
// 		return
// 	}

// 	ChannelList.TCPStorage[channelName].Offset += int64(byteSize)

// 	readByte = readByte[4:]

// 	messageMap = make(map[string]interface{})

// 	err = json.Unmarshal(readByte, &messageMap)

// 	if err != nil{
// 		go ChannelList.WriteLog(err.Error())
// 		return
// 	}
// }

func SendAck(messageMap map[string]interface{}, conn net.Conn){

	defer ChannelList.Handlepanic()

	var messageResp = make(map[string]interface{})

	messageResp["producer_id"] = messageMap["producer_id"].(string)

	jsonData, err := json.Marshal(messageResp)

	if err != nil{
		go ChannelList.WriteLog(err.Error())
		return
	}

	var packetBuffer bytes.Buffer

	buff := make([]byte, 4)

	AckMutex.Lock()

	binary.LittleEndian.PutUint32(buff, uint32(len(jsonData)))

	packetBuffer.Write(buff)

	packetBuffer.Write(jsonData)

	var counter = 0

	RETRY: _, err = conn.Write(packetBuffer.Bytes())

	AckMutex.Unlock()

	if err != nil && counter <= 5{

		counter += 1

		goto RETRY

	}
}

func WriteMongodbData(_id int64, jsonData []byte, channelName string, byteLen int, writeDataCallback chan bool){

	// MongoDBLock.Lock()
	// defer MongoDBLock.Unlock()

	defer ChannelList.Handlepanic()

	var packetBuffer bytes.Buffer

	buff := make([]byte, 4)

	binary.LittleEndian.PutUint32(buff, uint32(byteLen))

	packetBuffer.Write(buff)

	packetBuffer.Write(jsonData)

	var oneDoc = make(map[string]interface{})

	oneDoc["offsetID"] = _id
	oneDoc["cluster"] = *ChannelList.ConfigTCPObj.Server.TCP.ClusterName
	oneDoc["data"] = packetBuffer.Bytes()

	var counter = 0

	WRITEMONGODB:

		var status, _ = MongoConnection.InsertOne(channelName, oneDoc)

		if !status && counter <= 5{
			
			counter += 1

			goto WRITEMONGODB

		}

	if counter >= 5{
		writeDataCallback <- false
	}else{
		writeDataCallback <- true
	}
}

func WriteData(jsonData []byte, channelName string, byteLen int, writeDataCallback chan bool){

	defer ChannelList.Handlepanic()

	var packetBuffer bytes.Buffer

	buff := make([]byte, 4)

	// FileWriteLock.Lock()

	binary.LittleEndian.PutUint32(buff, uint32(byteLen))

	packetBuffer.Write(buff)

	packetBuffer.Write(jsonData)

	var counter = 0
 
 	WRITEFILE:
		_, err := ChannelList.TCPStorage[channelName].FD.Write(packetBuffer.Bytes())

		// FileWriteLock.Unlock()
		
		if (err != nil && err != io.EOF ) && counter <= 5{
			go ChannelList.WriteLog(err.Error())

			counter += 1

			goto WRITEFILE
		}

	if counter >= 5{
		writeDataCallback <- false
	}else{
		writeDataCallback <- true
	}	
}

func WriteOffset(jsonData []byte, channelName string, byteLen int, _id string, writeCallback chan bool){

	// FileTableLock.Lock()
	// defer FileTableLock.Unlock()
	defer ChannelList.Handlepanic()
	
	var byteSize = (byteLen + 4)

	ChannelList.TCPStorage[channelName].Offset += int64(byteSize)

	var offset = strconv.FormatInt(ChannelList.TCPStorage[channelName].Offset, 10)

	var offsetString = *ChannelList.ConfigTCPObj.Server.TCP.ClusterName+"|"+offset+"|"+_id+"\r\n"

	var packetBuffer bytes.Buffer

	buff := make([]byte, 2)

	binary.LittleEndian.PutUint16(buff, uint16(len(offsetString)))

	packetBuffer.Write(buff)
	packetBuffer.Write([]byte(offsetString))

	var counter = 0

	WRITEOFFSET:
		_, err := ChannelList.TCPStorage[channelName].TableFD.Write(packetBuffer.Bytes())

		if (err != nil && err != io.EOF) && counter <= 5{

			go ChannelList.WriteLog(err.Error())

			counter += 1

			goto WRITEOFFSET

		}

	if counter >= 5{
		writeCallback <- false
	}else{
		writeCallback <- true
	}
}