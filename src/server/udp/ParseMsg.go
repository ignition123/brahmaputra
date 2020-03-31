package udp

import (
	"net"
	"time"
	"log"
	"ChannelList"
	"pojo"
	"ByteBuffer"
	"encoding/binary"
	"sync"
)

var SubscriberHashMapMtx sync.RWMutex
var SubscriberGroupMtx sync.RWMutex

func ParseMsg(packetSize int64, completePacket []byte, conn net.UDPConn, addr *net.UDPAddr, parseChan chan bool, writeCount int, subscriberMapName *string, channelMapName *string, messageMapType *string, groupMapName *string){

	defer ChannelList.Recover()

	var byteBuffer = ByteBuffer.Buffer{
		Endian:"big",
	}

	byteBuffer.Wrap(completePacket)

	var messageTypeByte = byteBuffer.GetShort() // 2
	var messageTypeLen = int(binary.BigEndian.Uint16(messageTypeByte))
	var messageType = string(byteBuffer.Get(messageTypeLen)) // messageTypeLen

	var channelNameByte = byteBuffer.GetShort() // 2
	var channelNameLen = int(binary.BigEndian.Uint16(channelNameByte))
	var channelName = string(byteBuffer.Get(channelNameLen)) // channelNameLen

	*channelMapName = channelName

	var packetObject = &pojo.PacketStruct{
		MessageTypeLen: messageTypeLen,
		MessageType: messageType,
		ChannelNameLen: channelNameLen,
		ChannelName: channelName,
		UDPAddr: addr,
	}

	if messageType == "heart_beat"{

		if channelName == ""{

			ThroughUDPClientError(conn, *packetObject, INVALID_MESSAGE)

			parseChan <- false

			return

		}

		go log.Println("HEART BEAT RECEIVED...")

		packetObject.UDPConn = conn
		
		// ChannelList.UDPStorage["heart_beat"].BucketData[0] <- packetObject

	}else if messageType == "publish"{

		var producer_idByte = byteBuffer.GetShort()
		var producer_idLen = int(binary.BigEndian.Uint16(producer_idByte))
		var producer_id = string(byteBuffer.Get(producer_idLen))

		var agentNameByte = byteBuffer.GetShort()
		var agentNameLen = int(binary.BigEndian.Uint16(agentNameByte))
		var agentName = string(byteBuffer.Get(agentNameLen))

		var ackStatusByte = byteBuffer.GetByte()

		var bodyPacketSize = packetSize - int64(2 + messageTypeLen + 2 + channelNameLen + 2 + producer_idLen + 2 + agentNameLen + 1)

		var bodyPacket = byteBuffer.Get(int(bodyPacketSize))

		if ackStatusByte[0] == 1{

			packetObject.ProducerAck = true

		}else{

			packetObject.ProducerAck = false
			
		}

		packetObject.Producer_idLen = producer_idLen

		packetObject.Producer_id = producer_id

		packetObject.AgentNameLen = agentNameLen

		packetObject.AgentName = agentName

		packetObject.BodyBB = bodyPacket

		if channelName == ""{

			ThroughUDPClientError(conn, *packetObject, INVALID_MESSAGE)

			parseChan <- false

			return
		}

		if ChannelList.UDPStorage[channelName] == nil{

			ThroughUDPClientError(conn, *packetObject, INVALID_CHANNEL)

			parseChan <- false

			return

		}

		currentTime := time.Now()

		nanoEpoch := currentTime.UnixNano()

		packetObject.Id = nanoEpoch

		packetObject.UDPConn = conn

		if ChannelList.UDPStorage[channelName].ChannelStorageType == "persistent"{

			if *ChannelList.ConfigUDPObj.Storage.File.Active{

				go WriteData(*packetObject, writeCount)

				message, ok := <-ChannelList.UDPStorage[channelName].WriteCallback

				if ok{
							
					if !message{

						ThroughUDPClientError(conn, *packetObject, LOG_WRITE_FAILURE)

						parseChan <- false

						return
					}
				}

			}else{

				ThroughUDPClientError(conn, *packetObject, PERSISTENT_CONFIG_ERROR)

				parseChan <- false
				
				return

			}	

		}else{

			ThroughUDPClientError(conn, *packetObject, ONLY_PERSISTENT_SUPPORT)

			parseChan <- false

			return
		}

		parseChan <- true

	}else if messageType == "subscribe"{

		if channelName == ""{

			ThroughUDPClientError(conn, *packetObject, INVALID_MESSAGE)

			parseChan <- false

			return
		}

		if ChannelList.UDPStorage[channelName] == nil{

			ThroughUDPClientError(conn, *packetObject, INVALID_CHANNEL)

			parseChan <- false

			return
		}

		// bytes for start from flag

		var startFromByte = byteBuffer.GetShort() //2
		var startFromLen = binary.BigEndian.Uint16(startFromByte)
		var start_from = string(byteBuffer.Get(int(startFromLen))) // startFromLen

		// bytes for subscriber name 

		var subscriberNameByte = byteBuffer.GetShort() //2
		var subscriberNameLen = binary.BigEndian.Uint16(subscriberNameByte)
		var subscriberName = string(byteBuffer.Get(int(subscriberNameLen)))

		// bytes for subscriber group name

		var subscriberTypeByte = byteBuffer.GetShort() //2
		var subscriberTypeLen = binary.BigEndian.Uint16(subscriberTypeByte)

		packetObject.StartFromLen = int(startFromLen)

		packetObject.Start_from = start_from

		packetObject.SubscriberNameLen = int(subscriberNameLen)

		packetObject.SubscriberName = subscriberName

		packetObject.SubscriberTypeLen = int(subscriberTypeLen)

		packetObject.UDPConn = conn

		packetObject.ActiveMode = true

		// checking for group name existence

		if ChannelList.UDPStorage[channelName].ChannelStorageType == "persistent"{

			if subscriberTypeLen > 0{

				var groupName = string(byteBuffer.Get(int(subscriberTypeLen))) 

				packetObject.GroupName = groupName

				if !*ChannelList.ConfigUDPObj.Storage.File.Active{

					ThroughUDPClientError(conn, *packetObject, PERSISTENT_CONFIG_ERROR)

					parseChan <- false
					
					return

				}

				*subscriberMapName = channelName+subscriberName+groupName
				*messageMapType = messageType
				*groupMapName = groupName
    				
    			StoreUDPChannelSubscriberList(channelName, channelName+subscriberName+groupName, true)

				// checking for key already in the hashmap

				groupLen := GetChannelGrpMapLen(channelName, groupName)

				if groupLen > 0{

					if groupLen == ChannelList.UDPStorage[channelName].PartitionCount{

						ThroughUDPClientError(conn, *packetObject, SUBSCRIBER_FULL)

						parseChan <- false

						return

					}

					AddNewClientToGrp(channelName, groupName, *packetObject)

				}else{

					if start_from != "BEGINNING" && start_from != "NOPULL" && start_from != "LASTRECEIVED"{

			    		ThroughUDPClientError(conn, *packetObject, INVALID_PULL_FLAG)

						parseChan <- false

						return
			    	}

    				RenewSub(channelName, groupName)

    				AddNewClientToGrp(channelName, groupName, *packetObject)

		    		go SubscribeGroupChannel(channelName, groupName, *packetObject, start_from)
				}

			}else{

				if !*ChannelList.ConfigUDPObj.Storage.File.Active{

					ThroughUDPClientError(conn, *packetObject, PERSISTENT_CONFIG_ERROR)

					parseChan <- false
					
					return

				}

		    	if start_from != "BEGINNING" && start_from != "NOPULL" && start_from != "LASTRECEIVED"{

		    		ThroughUDPClientError(conn, *packetObject, INVALID_PULL_FLAG)

					parseChan <- false

					return
		    	}

    			*subscriberMapName = channelName+subscriberName
    			*messageMapType = messageType

    			StoreUDPChannelSubscriberList(channelName, channelName+subscriberName, true)

				go SubscribeChannel(conn, *packetObject, start_from)

			}

		}else{

			ChannelList.UDPSocketDetails[channelName] = append(ChannelList.UDPSocketDetails[channelName], packetObject) 
		}
		
		parseChan <- true 

	}else{

		ThroughUDPClientError(conn, *packetObject, INVALID_AGENT)

		parseChan <- false

		return
	}	
}

func WriteData(packet pojo.PacketStruct, writeCount int){

	defer ChannelList.Recover()

	var byteBuffer = ByteBuffer.Buffer{
		Endian:"big",
	}

	// totalLen + messageTypelen + messageType + channelNameLen + channelName + producerIdLen + producerID + agentNameLen + agentName + _id + totalBytePacket

	var totalByteLen = 2 + packet.MessageTypeLen + 2 + packet.ChannelNameLen + 2 + packet.Producer_idLen + 2 + packet.AgentNameLen + 8 + len(packet.BodyBB)

	byteBuffer.PutLong(totalByteLen)

	byteBuffer.PutShort(packet.MessageTypeLen)

	byteBuffer.Put([]byte(packet.MessageType))

	byteBuffer.PutShort(packet.ChannelNameLen)

	byteBuffer.Put([]byte(packet.ChannelName))

	byteBuffer.PutShort(packet.Producer_idLen)

	byteBuffer.Put([]byte(packet.Producer_id))

	byteBuffer.PutShort(packet.AgentNameLen)

	byteBuffer.Put([]byte(packet.AgentName))

	byteBuffer.PutLong(int(packet.Id))

	byteBuffer.Put(packet.BodyBB)

	_, err := ChannelList.UDPStorage[packet.ChannelName].FD[writeCount].Write(byteBuffer.Array())


	if (err != nil){
		go ChannelList.WriteLog(err.Error())
		ChannelList.UDPStorage[packet.ChannelName].WriteCallback <- false
		return
	}

	ChannelList.UDPStorage[packet.ChannelName].WriteCallback <- true
}

