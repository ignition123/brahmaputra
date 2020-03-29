package server

import (
	"encoding/binary"
	"net"
	_"time"
	"ChannelList"
	_"sync"
	_"log"
)

var closeTCP = false

func allZero(s []byte) bool {

	defer ChannelList.Recover()
	
	for _, v := range s {
		if v != 0 {
			return false
		}
	}
	return true
}

type ChannelStatus struct{
	Status bool
}

func HandleRequest(conn net.TCPConn) {
	
	defer ChannelList.Recover()

	defer conn.Close()

	parseChan := make(chan bool, 1)

	var counterRequest = 0

	var writeCount = 0

	var subscriberMapName string

	var channelMapName string

	var messageMapType string

	var groupMapName = ""

	for {

		if closeTCP{
			go ChannelList.WriteLog("Closing all current sockets...")
			conn.Close()
			break
		}

		sizeBuf := make([]byte, 8)

		conn.Read(sizeBuf)

		packetSize := binary.BigEndian.Uint64(sizeBuf)

		if packetSize < 0 {
			continue
		}

		completePacket := make([]byte, packetSize)

		conn.Read(completePacket)

		if allZero(completePacket) {

			go ChannelList.WriteLog("Connection closed...")
			break
		}

		if ChannelList.TCPStorage[channelMapName] != nil{

			if writeCount >= ChannelList.TCPStorage[channelMapName].PartitionCount{

				writeCount = 0

			}

		}else{

			writeCount = 0
		}
		
		go ParseMsg(int64(packetSize), completePacket, conn, parseChan, writeCount, &counterRequest, &subscriberMapName, &channelMapName, &messageMapType, &groupMapName)

		writeCount += 1

		<-parseChan
	}

	if messageMapType == "subscribe"{

		DeleteTCPChannelSubscriberList(channelMapName, subscriberMapName)

		if groupMapName != ""{

			//get consumer group Length

			var consumerGroupLen = GetChannelGrpMapLen(channelMapName, groupMapName)

			if consumerGroupLen > 0{

				// Delete Group Member

				RemoveGroupMember(channelMapName, groupMapName, subscriberMapName)

			}
		}
	}

}


func CloseTCPServers(){
	
	defer ChannelList.Recover()

	ChannelList.WriteLog("Closing tcp socket...")

	closeTCP = true
}