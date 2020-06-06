package tcp

/*
	Methods to parse request from client source
*/

// importing modules

import (
	"net"
	"time"
	"log"
	"ChannelList"
	"pojo"
	"ByteBuffer"
	"encoding/binary"
)

// method to parse message from socket client

func ParseMsg(packetSize int64, completePacket []byte, conn net.TCPConn, parseChan chan bool, socketDisconnect *bool, writeCount int, clientObj *pojo.ClientObject){

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

	// create object of struct PacketStruct

	packetObject := &pojo.PacketStruct{
		MessageTypeLen: messageTypeLen,
		MessageType: messageType,
		ChannelNameLen: channelNameLen,
		ChannelName: channelName,
	}

	// checking the message type

	if messageType == "heart_beat"{

		if channelName == ""{

			ThroughClientError(conn, INVALID_MESSAGE)

			parseChan <- false

			return

		}

		go log.Println("HEART BEAT RECEIVED...")

		packetObject.Conn = conn
		
		ChannelList.TCPStorage["heart_beat"].BucketData[0] <- packetObject

	}else if messageType == "publish"{

		// if message type is publish

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

			ThroughClientError(conn, INVALID_MESSAGE)

			parseChan <- false

			return
		}

		// if channel name does not exists in the system then error

		if ChannelList.TCPStorage[channelName] == nil{

			ThroughClientError(conn, INVALID_CHANNEL)

			parseChan <- false

			return

		}

		// getting current time

		currentTime := time.Now()

		// getting current time in nano second

		nanoEpoch := currentTime.UnixNano()

		// setting the nanoEpoch as Id

		packetObject.Id = nanoEpoch

		// setting the socket object

		packetObject.Conn = conn

		// checking if the channel storage type is persistent
		
		if ChannelList.TCPStorage[channelName].ChannelStorageType == "persistent"{

			// checking if the storage file is active, other options databases like mongodb, mySQL, Cassandra, Hbase etc (current support only for files and mongodb)

			if *ChannelList.ConfigTCPObj.Storage.File.Active{

				// appending data to file

				go WriteData(*packetObject, writeCount, clientObj)

				// waiting for callbacks

				message, ok := <-ChannelList.TCPStorage[channelName].WriteCallback

				if ok{
						
					// if message is false boolean then throw error

					if !message{

						ThroughClientError(conn, LOG_WRITE_FAILURE)

						parseChan <- false

						return
					}
				}

			}else{

				// if file active != true then error as mongodb is yet not implemented

				ThroughClientError(conn, PERSISTENT_CONFIG_ERROR)

				parseChan <- false
				
				return

			}	

		}else{

			// if counterRequest == worker means it has reached to max limit then the counterRequest will be set to zero

			if clientObj.CounterRequest == ChannelList.TCPStorage[channelName].Worker{

				clientObj.CounterRequest = 0
			}

			// writing packet object to bucket channels it executed in case of inmemory channels

			ChannelList.TCPStorage[channelName].BucketData[clientObj.CounterRequest] <- packetObject

			// request Counter incremented by 1

			clientObj.CounterRequest += 1
		}

		// if producer Ack == True then acknowledgement is sent to the producer

		if packetObject.ProducerAck{

			go ChannelMethod.SendAck(*packetObject, ChannelList.TCPStorage[channelName].WriteCallback)

			<-ChannelList.TCPStorage[channelName].WriteCallback

		}

		// callback sent to the channel of parseMsg Method

		parseChan <- true

	}else if messageType == "subscribe"{

		// if messageType is for subscriber then this snippet is executed

		// checking for channelName emptyness

		if channelName == ""{

			ThroughClientError(conn, INVALID_MESSAGE)

			parseChan <- false

			return
		}

		// checking if channel name does not exists in the system

		if ChannelList.TCPStorage[channelName] == nil{

			ThroughClientError(conn, INVALID_CHANNEL)

			parseChan <- false

			return
		}

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

		packetObject.Conn = conn

		packetObject.ActiveMode = true

		// loading the tcp channel subscriber list

		keyFound := LoadTCPChannelSubscriberList(channelName, channelName+subscriberName)

		// if same subscriber found then it will not allow to the subscriber to listen, all subscriber must have unique name

		if keyFound{

			ThroughClientError(conn, SAME_SUBSCRIBER_DETECTED)

			parseChan <- false

			return
		   
		}

		// checking for group name existence

		if ChannelList.TCPStorage[channelName].ChannelStorageType == "persistent"{

			// if the subscriberType length > 0

			if subscriberTypeLen > 0{

				// getting the groupName

				groupName := string(byteBuffer.Get(int(subscriberTypeLen))) 

				packetObject.GroupName = groupName

				// checking if the file type is active is case of persistent to read from file else error

				if !*ChannelList.ConfigTCPObj.Storage.File.Active{

					ThroughClientError(conn, PERSISTENT_CONFIG_ERROR)

					parseChan <- false
					
					return

				}

				// setting the subscriber mapName, messageType and groupName pointers for reference

				clientObj.SubscriberMapName = channelName+subscriberName+groupName
				clientObj.MessageMapType = messageType
				clientObj.GroupMapName = groupName

				// storing the subscriber in the subscriber list
    				
    			StoreTCPChannelSubscriberList(channelName, channelName+subscriberName+groupName, true)

				// checking for key already in the hashmap

				groupLen := GetChannelGrpMapLen(channelName, groupName)

				if groupLen > 0{

					// if group length > 0

					// if group length == channel partition count then error

					if groupLen == ChannelList.TCPStorage[channelName].PartitionCount{

						ThroughClientError(conn, SUBSCRIBER_FULL)

						parseChan <- false

						return

					}

					// adding new client to group

					AddNewClientToGrp(channelName, groupName, *packetObject)

				}else{

					// checking if the start_from has invalid value not amoung the three

					if start_from != "BEGINNING" && start_from != "NOPULL" && start_from != "LASTRECEIVED"{

			    		ThroughClientError(conn, INVALID_PULL_FLAG)

						parseChan <- false

						return
			    	}

			    	// renew subscriber

    				RenewSub(channelName, groupName)

    				// add new client to group

    				AddNewClientToGrp(channelName, groupName, *packetObject)

    				// infinitely listening to the log file for changes and publishing to subscriber, subscriber group list

		    		go SubscribeGroupChannel(channelName, groupName, *packetObject, start_from, socketDisconnect)
				}

			}else{

				// checking if file type is active

				if !*ChannelList.ConfigTCPObj.Storage.File.Active{

					ThroughClientError(conn, PERSISTENT_CONFIG_ERROR)

					parseChan <- false
					
					return

				}

				// checking if the start_from has invalid value not amoung the three

		    	if start_from != "BEGINNING" && start_from != "NOPULL" && start_from != "LASTRECEIVED"{

		    		ThroughClientError(conn, INVALID_PULL_FLAG)

					parseChan <- false

					return
		    	}

		    	// setting the subscriber mapName, messageType and groupName pointers for reference

    			clientObj.SubscriberMapName = channelName+subscriberName
    			clientObj.MessageMapType = messageType

    			// storing the client in the subscriber list

    			StoreTCPChannelSubscriberList(channelName, channelName+subscriberName, true)

    			// infinitely listening to the log file for changes and publishing to subscriber individually listening

				go SubscribeChannel(conn, *packetObject, start_from, socketDisconnect)

			}

		}else{

			// setting the subscriber mapName, messageType and groupName pointers for reference

			clientObj.SubscriberMapName = channelName+subscriberName
			clientObj.MessageMapType = messageType

			// adding new subscriber to inmemory client

			AppendNewClientInmemory(channelName, clientObj.SubscriberMapName, packetObject)

		}
		
		parseChan <- true 

	}else{

		ThroughClientError(conn, INVALID_AGENT)

		parseChan <- false

		return
	}	
}
