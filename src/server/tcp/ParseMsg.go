package tcp

/*
	Methods to parse request from client source
*/

// importing modules

import (
	"net"
	"time"
	_"log"
	"ChannelList"
	"pojo"
	"ByteBuffer"
	"encoding/binary"
)

// method to parse message from socket client

func parseMsg(packetSize int64, completePacket []byte, conn net.TCPConn, parseChan chan bool, clientObj *pojo.ClientObject, writeCount int){

	defer ChannelList.Recover()

	// creating byte buffer in big endian

	byteBuffer := ByteBuffer.Buffer{
		Endian:"big",
	}

	// wrapping the complete packet received from the tcp socket

	byteBuffer.Wrap(completePacket)

	// parsing the message type

	messageTypeLen := int(binary.BigEndian.Uint16(byteBuffer.GetShort()))
	messageType := string(byteBuffer.Get(messageTypeLen)) // messageTypeLen

	// parsing the channel name

	channelNameLen := int(binary.BigEndian.Uint16(byteBuffer.GetShort()))
	channelName := string(byteBuffer.Get(channelNameLen)) // channelNameLen

	// setting the channelName to a channelMapName pointer for reference of memory address, used when the socket disconnects

	clientObj.ChannelMapName = channelName

	// checking for channelName emptyness

	if channelName == ""{

		ChannelList.ThroughClientError(conn, ChannelList.INVALID_MESSAGE)

		return
	}

	// checking if channel name does not exists in the system

	_, chanExits := pojo.SubscriberObj[channelName]

	if !chanExits{

		ChannelList.ThroughClientError(conn, ChannelList.INVALID_CHANNEL)

		return
	}

	// create object of struct PacketStruct

	packetObject := pojo.PacketStruct{
		MessageTypeLen: messageTypeLen,
		MessageType: messageType,
		ChannelNameLen: channelNameLen,
		ChannelName: channelName,
	}

	if messageType == "publish"{

		handleProducerMessage(byteBuffer, messageType, clientObj, channelName, conn, &packetObject, packetSize, messageTypeLen, channelNameLen, writeCount)

	}else if messageType == "subscribe"{

		if pojo.SubscriberObj[channelName].Channel.ChannelStorageType == "inmemory"{

			handleSubscriberInmemoryMessage(byteBuffer, messageType, clientObj, channelName, conn, &packetObject, packetSize, messageTypeLen, channelNameLen)
				
		}else{

			handleSubscriberPersistentMessage(byteBuffer, messageType, clientObj, channelName, conn, &packetObject, packetSize, messageTypeLen, channelNameLen)

		}
		
	}

	parseChan <- true	
}

func handleSubscriberPersistentMessage(byteBuffer ByteBuffer.Buffer, messageType string, clientObj *pojo.ClientObject, channelName string, conn net.TCPConn, packetObject *pojo.PacketStruct, packetSize int64, messageTypeLen int, channelNameLen int){

	defer ChannelList.Recover()

	// bytes for start from flag

	startFromLen := binary.BigEndian.Uint16(byteBuffer.GetShort())
	start_from := string(byteBuffer.Get(int(startFromLen))) // startFromLen

	// bytes for subscriber name 

	subscriberNameLen := binary.BigEndian.Uint16(byteBuffer.GetShort())
	subscriberName := string(byteBuffer.Get(int(subscriberNameLen)))

	// bytes for subscriber group name

	subscriberTypeLen := binary.BigEndian.Uint16(byteBuffer.GetShort())

	// setting variables to the packetObject

	packetObject.StartFromLen = int(startFromLen)

	packetObject.Start_from = start_from

	packetObject.SubscriberNameLen = int(subscriberNameLen)

	packetObject.SubscriberName = subscriberName

	packetObject.SubscriberTypeLen = int(subscriberTypeLen)

	packetObject.ActiveMode = true

	clientObj.Conn = conn

	clientObj.MessageMapType = messageType

	// checking if the start_from has invalid value not amoung the three

	if start_from != "BEGINNING" && start_from != "NOPULL" && start_from != "LASTRECEIVED"{

		ChannelList.ThroughClientError(conn, ChannelList.INVALID_PULL_FLAG)

		return
	}

	// checking if file type is active

	if !*ChannelList.ConfigTCPObj.Storage.File.Active{

		ChannelList.ThroughClientError(conn, ChannelList.PERSISTENT_CONFIG_ERROR)
		
		return

	}

	if subscriberTypeLen > 0{

		// getting the groupName

		packetObject.GroupName = string(byteBuffer.Get(int(subscriberTypeLen))) 

		// setting the subscriber mapName, messageType and groupName pointers for reference

		clientObj.SubscriberMapName = channelName+subscriberName+packetObject.GroupName
		
		clientObj.GroupMapName = packetObject.GroupName

		// storing the subscriber in the subscriber list
			
		if ChannelList.GetChannelGrpMapLen(channelName, packetObject.GroupName) == 0{

			go SubscribeGroupChannel(channelName, packetObject.GroupName, packetObject, clientObj, start_from)

		}

		pojo.SubscriberObj[channelName].Register <- clientObj

	}else{

    	// setting the subscriber mapName, messageType and groupName pointers for reference

		clientObj.SubscriberMapName = channelName+subscriberName

		// storing the client in the subscriber list

		pojo.SubscriberObj[channelName].Register <- clientObj

		// infinitely listening to the log file for changes and publishing to subscriber individually listening

		go SubscriberSinglePersistent(clientObj, packetObject, start_from)

	}

}

func handleSubscriberInmemoryMessage(byteBuffer ByteBuffer.Buffer, messageType string, clientObj *pojo.ClientObject, channelName string, conn net.TCPConn, packetObject *pojo.PacketStruct, packetSize int64, messageTypeLen int, channelNameLen int){

	defer ChannelList.Recover()

	// bytes for start from flag

	startFromLen := binary.BigEndian.Uint16(byteBuffer.GetShort())
	start_from := string(byteBuffer.Get(int(startFromLen))) // startFromLen

	// bytes for subscriber name 

	subscriberNameLen := binary.BigEndian.Uint16(byteBuffer.GetShort())
	subscriberName := string(byteBuffer.Get(int(subscriberNameLen)))

	// bytes for subscriber group name

	subscriberTypeLen := binary.BigEndian.Uint16(byteBuffer.GetShort())

	// setting variables to the packetObject

	packetObject.StartFromLen = int(startFromLen)

	packetObject.Start_from = start_from

	packetObject.SubscriberNameLen = int(subscriberNameLen)

	packetObject.SubscriberName = subscriberName

	packetObject.SubscriberTypeLen = int(subscriberTypeLen)

	packetObject.ActiveMode = true

	clientObj.Conn = conn

	// setting the subscriber mapName, messageType and groupName pointers for reference

	clientObj.SubscriberMapName = channelName+subscriberName
	clientObj.MessageMapType = messageType

	pojo.SubscriberObj[channelName].Register <- clientObj

	go SubscriberInmemory(clientObj)

}

func handleProducerMessage(byteBuffer ByteBuffer.Buffer, messageType string, clientObj *pojo.ClientObject, channelName string, conn net.TCPConn, packetObject *pojo.PacketStruct, packetSize int64, messageTypeLen int, channelNameLen int, writeCount int){

	defer ChannelList.Recover()

	// parsing the producerId

	producer_idLen := int(binary.BigEndian.Uint16(byteBuffer.GetShort()))
	producer_id := string(byteBuffer.Get(producer_idLen))

	// parsing the agentName

	agentNameLen := int(binary.BigEndian.Uint16(byteBuffer.GetShort()))
	agentName := string(byteBuffer.Get(agentNameLen))

	// producer acknowledgement byte packet

	ackStatusByte := byteBuffer.GetByte()

	// compression type byte packet

	compression := byteBuffer.GetByte()
	packetObject.CompressionType = compression[0]

	// getting the actual body packet size

	bodyPacketSize := packetSize - int64(2 + messageTypeLen + 2 + channelNameLen + 2 + producer_idLen + 2 + agentNameLen + 1 + 1)

	// actual body packet

	bodyPacket := byteBuffer.Get(int(bodyPacketSize))

	// if ack flag is 1 that means producer needs acknowledgement

	if ackStatusByte[0] == 1{

		packetObject.ProducerAck = true

	}else{

		packetObject.ProducerAck = false
		
	}

	// setting producerLen

	packetObject.Producer_idLen = producer_idLen

	// settig producerId

	packetObject.Producer_id = producer_id

	// setting agentName length

	packetObject.AgentNameLen = agentNameLen

	// setting agent name

	packetObject.AgentName = agentName

	// setting actual body packet

	packetObject.BodyBB = bodyPacket

	// if channel name is empty then error

	if channelName == ""{

		ChannelList.ThroughClientError(conn, ChannelList.INVALID_MESSAGE)

		return
	}

	// if channel name does not exists in the system then error

	if pojo.SubscriberObj[channelName] == nil{

		ChannelList.ThroughClientError(conn, ChannelList.INVALID_CHANNEL)

		return

	}

	// getting current time

	currentTime := time.Now()

	// getting current time in nano second

	nanoEpoch := currentTime.UnixNano()

	// setting the nanoEpoch as Id

	packetObject.Id = nanoEpoch

	// setting the socket object

	clientObj.Conn = conn

	if pojo.SubscriberObj[channelName].Channel.ChannelStorageType == "inmemory"{

		pojo.SubscriberObj[channelName].BroadCast <- packetObject

	}else{

		if *ChannelList.ConfigTCPObj.Storage.File.Active{

			if !WriteData(*packetObject, writeCount, clientObj){

				ChannelList.ThroughClientError(conn, ChannelList.LOG_WRITE_FAILURE)

			}

		}

	}

	if packetObject.ProducerAck{

		ChannelList.SendAck(packetObject, clientObj)
	}

}