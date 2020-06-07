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
	"pojo"
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

	// creating client object

	clientObj := pojo.ClientObject{
		CounterRequest: 0,
		SubscriberMapName: "",
		ChannelMapName: "",
		MessageMapType: "",
		GroupMapName: "",
	}

	// writeCount for round robin writes in file

	writeCount := 0

	// socket disconnect variable to exits subscriber loop from file
	socketDisconnect := false

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

		packetSize := int64(binary.BigEndian.Uint64(sizeBuf))

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

		// checking if the clientObj.ChannelMapName exists and is not nul

		if ChannelList.TCPStorage[clientObj.ChannelMapName] != nil{

			// if writeCount >= ChannelList.TCPStorage[clientObj.ChannelMapName].PartitionCount then writeCount = 0 this is used to load balance in writing in multiple files using round robin algorithm

			if writeCount >= ChannelList.TCPStorage[clientObj.ChannelMapName].PartitionCount{

				writeCount = 0

			}

		}else{

			writeCount = 0
		}

		// calling the parseMessage method and waiting for callback
		
		go parseMsg(packetSize, completePacket, conn, parseChan, &socketDisconnect, writeCount,&clientObj)

		// after callback incrementing writeCount = 1

		writeCount += 1

		<-parseChan
	}

	socketDisconnect = true

	// if loop breaks then checking the socket type publisher or subscriber

	if clientObj.MessageMapType == "subscribe"{

		// if messageType == subscribe

		// deleting the subscriber from ChannelList client hashmap

		deleteInmemoryChannelList(clientObj.ChannelMapName, clientObj.SubscriberMapName)

		// deleting the subscriber from tcp subscriber list

		deleteTCPChannelSubscriberList(clientObj.ChannelMapName, clientObj.SubscriberMapName)

		// checking if the subscriber is a part of any group

		if clientObj.GroupMapName != ""{

			//get consumer group Length

			var consumerGroupLen = getChannelGrpMapLen(clientObj.ChannelMapName, clientObj.GroupMapName)

			if consumerGroupLen > 0{

				// Delete Group Member

				removeGroupMember(clientObj.ChannelMapName, clientObj.GroupMapName, clientObj.SubscriberMapName)

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