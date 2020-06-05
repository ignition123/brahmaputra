package tcp

/*
	Handling tcp socket clients
*/

// importing modules

import (
	"encoding/binary"
	"net"
	"ChannelList"
	"io"
)

// creating a closeTCP variale with boolean value false, it is set to true when the application crashes it will close all tcp client

var closeTCP = false

// method to check if the packet is having 0 bytes in packets

func allZero(s []byte) bool {

	defer ChannelList.Recover()
	
	for _, v := range s {
		if v != 0 {
			return false
		}
	}
	return true
}

// handling the tcp socket

func HandleRequest(conn net.TCPConn) {
	
	defer ChannelList.Recover()

	// closing tcp connection if loop ends

	defer conn.Close()

	// creating a boolean channel to sync threads

	parseChan := make(chan bool, 1)
	defer close(parseChan)

	// creating local variables to manager counters

	counterRequest := 0
	writeCount := 0
	groupMapName := ""

	// socket disconnect variable to exits subscriber loop from file
	socketDisconnect := false

	var subscriberMapName string
	var channelMapName string
	var messageMapType string

	// staring infinite loop

	for {

		// if closeTCP == true then all connections will be closed
		
		if closeTCP{
			go ChannelList.WriteLog("Closing all current sockets...")
			conn.Close()
			break
		}

		// creating a 8 byte buffer array

		sizeBuf := make([]byte, 8)

		// reading from tcp sockets

		_, err := conn.Read(sizeBuf)

		// checking the error type

		if err == io.EOF{
			
			go ChannelList.WriteLog("Connection closed...")

			break

		}

		if err != nil{

			go ChannelList.WriteLog(err.Error())

			break

		}

		// converting the packet size to int64

		packetSize := binary.BigEndian.Uint64(sizeBuf)

		if packetSize < 0 {
			continue
		}

		// reading more bytes from tcp pipe of packetSize length

		completePacket := make([]byte, packetSize)

		_, err = conn.Read(completePacket)

		// checking error type

		if err == io.EOF{

			go ChannelList.WriteLog("Connection closed...")
			
			break

		}

		if err != nil{

			go ChannelList.WriteLog(err.Error())

			break

		}

		// checking if the packet contains 0 buffers

		if allZero(completePacket) {

			go ChannelList.WriteLog("Connection closed...")
			
			break
		}

		// checking if the channelMapName exists and is not nul

		if ChannelList.TCPStorage[channelMapName] != nil{

			// if writeCount >= ChannelList.TCPStorage[channelMapName].PartitionCount then writeCount = 0 this is used to load balance in writing in multiple files using round robin algorithm

			if writeCount >= ChannelList.TCPStorage[channelMapName].PartitionCount{

				writeCount = 0

			}

		}else{

			writeCount = 0
		}

		// calling the parseMessage method and waiting for callback
		
		go ParseMsg(int64(packetSize), completePacket, conn, parseChan, writeCount, &counterRequest, &subscriberMapName, &channelMapName, &messageMapType, &groupMapName, &socketDisconnect)

		// after callback incrementing writeCount = 1

		writeCount += 1

		<-parseChan
	}

	socketDisconnect = true

	// if loop breaks then checking the socket type publisher or subscriber

	if messageMapType == "subscribe"{

		// if messageType == subscribe

		// deleting the subscriber from ChannelList client hashmap

		DeleteInmemoryChannelList(channelMapName, subscriberMapName)

		// deleting the subscriber from tcp subscriber list

		DeleteTCPChannelSubscriberList(channelMapName, subscriberMapName)

		// checking if the subscriber is a part of any group

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

// closing the TCP Server

func CloseTCPServers(){
	
	defer ChannelList.Recover()

	ChannelList.WriteLog("Closing tcp socket...")

	closeTCP = true
}