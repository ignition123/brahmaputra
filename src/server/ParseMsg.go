package server

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

var SubscriberHashMapMtx sync.Mutex

func ParseMsg(packetSize int64, completePacket []byte, conn net.TCPConn, parseChan chan bool, writeCount *int, counterRequest *int, subscriberMapName *string){

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

	var packetObject = &pojo.PacketStruct{
		MessageTypeLen: messageTypeLen,
		MessageType: messageType,
		ChannelNameLen: channelNameLen,
		ChannelName: channelName,
	}

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

		var producer_idByte = byteBuffer.GetShort()
		var producer_idLen = int(binary.BigEndian.Uint16(producer_idByte))
		var producer_id = string(byteBuffer.Get(producer_idLen))

		var agentNameByte = byteBuffer.GetShort()
		var agentNameLen = int(binary.BigEndian.Uint16(agentNameByte))
		var agentName = string(byteBuffer.Get(agentNameLen))

		var bodyPacketSize = packetSize - int64(2 + messageTypeLen + 2 + channelNameLen + 2 + producer_idLen + 2 + agentNameLen)

		var bodyPacket = byteBuffer.Get(int(bodyPacketSize))

		packetObject.Producer_idLen = producer_idLen

		packetObject.Producer_id = producer_id

		packetObject.AgentNameLen = agentNameLen

		packetObject.AgentName = agentName

		packetObject.BodyBB = bodyPacket

		if channelName == ""{

			ThroughClientError(conn, INVALID_MESSAGE)

			parseChan <- false

			return
		}

		if ChannelList.TCPStorage[channelName] == nil{

			ThroughClientError(conn, INVALID_CHANNEL)

			parseChan <- false

			return

		}

		currentTime := time.Now()

		nanoEpoch := currentTime.UnixNano()

		packetObject.Id = nanoEpoch
		
		if ChannelList.TCPStorage[channelName].ChannelStorageType == "persistent"{

			if *ChannelList.ConfigTCPObj.Storage.File.Active{

				go WriteData(*packetObject, writeCount)

				message, ok := <-ChannelList.TCPStorage[channelName].WriteCallback

				if ok{
							
					if !message{

						ThroughClientError(conn, LOG_WRITE_FAILURE)

						parseChan <- false

						return
					}
				}

			}else{

				ThroughClientError(conn, PERSISTENT_CONFIG_ERROR)

				parseChan <- false
				
				return

			}	
		}

		packetObject.Conn = conn

		if *counterRequest == ChannelList.TCPStorage[channelName].Worker{

			*counterRequest = 0
		}

		ChannelList.TCPStorage[channelName].BucketData[*counterRequest] <- packetObject

		*counterRequest += 1

		parseChan <- true

	}else if messageType == "subscribe"{

		if channelName == ""{

			ThroughClientError(conn, INVALID_MESSAGE)

			parseChan <- false

			return
		}

		if ChannelList.TCPStorage[channelName] == nil{

			ThroughClientError(conn, INVALID_CHANNEL)

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

		packetObject.Conn = conn

		// checking for group name existence

		if subscriberTypeLen > 0{

			var groupName = string(byteBuffer.Get(int(subscriberTypeLen))) 

			packetObject.GroupName = groupName

			groupKey, groupFound := ChannelList.TCPSubscriberGroup[channelName][groupName]

			if groupFound{

				if len(groupKey) == ChannelList.TCPStorage[channelName].PartitionCount{

					ThroughClientError(conn, SUBSCRIBER_FULL)

					parseChan <- false

					return

				}

			}

			if ChannelList.TCPStorage[channelName].ChannelStorageType == "persistent"{

				if !*ChannelList.ConfigTCPObj.Storage.File.Active{

					ThroughClientError(conn, PERSISTENT_CONFIG_ERROR)

					parseChan <- false
					
					return

				}

		    	if len(ChannelList.TCPSubscriberGroup[channelName][groupName]) <= 0{

		    		if start_from != "BEGINNING" && start_from != "NOPULL" && start_from != "LASTRECEIVED"{

			    		ThroughClientError(conn, INVALID_PULL_FLAG)

						parseChan <- false

						return
			    	}

    				ChannelList.TCPSubscriberGroup[channelName][groupName] = make(map[int]*pojo.PacketStruct)

		    		go SubscribeGroupChannel(channelName, groupName, *packetObject, start_from)

		    	}

			}

			var groupLen = len(ChannelList.TCPSubscriberGroup[channelName][groupName])

			if groupLen <= 0{

				ChannelList.TCPSubscriberGroup[channelName][groupName][0] = packetObject

			}else{

				ChannelList.TCPSubscriberGroup[channelName][groupName][groupLen] = packetObject

			}

		}else{

			_, keyFound := ChannelList.TCPChannelSubscriberList[channelName+subscriberName]

			if keyFound{

				ThroughClientError(conn, SAME_SUBSCRIBER_DETECTED)

				parseChan <- false

				return
			   
			}

			*subscriberMapName = channelName+subscriberName

			SubscriberHashMapMtx.Lock()
			ChannelList.TCPChannelSubscriberList[channelName+subscriberName] = true
			SubscriberHashMapMtx.Unlock()

			if ChannelList.TCPStorage[channelName].ChannelStorageType == "persistent"{

				if !*ChannelList.ConfigTCPObj.Storage.File.Active{

					ThroughClientError(conn, PERSISTENT_CONFIG_ERROR)

					parseChan <- false
					
					return

				}

		    	if start_from != "BEGINNING" && start_from != "NOPULL" && start_from != "LASTRECEIVED"{

		    		ThroughClientError(conn, INVALID_PULL_FLAG)

					parseChan <- false

					return
		    	}

				go SubscribeChannel(conn, *packetObject, start_from)

			}else{

				ChannelList.TCPSocketDetails[channelName] = append(ChannelList.TCPSocketDetails[channelName], packetObject) 

			}

		}
		
		parseChan <- true 

	}else{

		ThroughClientError(conn, INVALID_AGENT)

		parseChan <- false

		return
	}	
}

func WriteData(packet pojo.PacketStruct, writeCount *int){

	defer ChannelList.Recover()

	ChannelList.TCPStorage[packet.ChannelName].ChannelLock.Lock()

	if *writeCount == ChannelList.TCPStorage[packet.ChannelName].PartitionCount{

		*writeCount = 0

	}

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
 
	_, err := ChannelList.TCPStorage[packet.ChannelName].FD[*writeCount].Write(byteBuffer.Array())

	*writeCount += 1

	ChannelList.TCPStorage[packet.ChannelName].ChannelLock.Unlock()
	
	if (err != nil){
		go ChannelList.WriteLog(err.Error())
		ChannelList.TCPStorage[packet.ChannelName].WriteCallback <- false
		return
	}

	ChannelList.TCPStorage[packet.ChannelName].WriteCallback <- true
}

func ThroughClientError(conn net.TCPConn, message string){

	defer ChannelList.Recover()

	var totalByteLen = len(message)

	var byteSendBuffer = ByteBuffer.Buffer{
		Endian:"big",
	}

	byteSendBuffer.PutLong(totalByteLen) // total packet length

	byteSendBuffer.PutByte(byte(1)) // status code

	byteSendBuffer.Put([]byte(message)) // actual body

	_, err := conn.Write(byteSendBuffer.Array())

	if (err != nil){

		go ChannelList.WriteLog(err.Error())

	}

	go ChannelList.WriteLog(message)

	conn.Close()
}

func ThroughGroupError(channelName string, groupName string, message string){

	defer ChannelList.Recover()

	var totalByteLen = len(message)

	var byteSendBuffer = ByteBuffer.Buffer{
		Endian:"big",
	}

	byteSendBuffer.PutLong(totalByteLen) // total packet length

	byteSendBuffer.PutByte(byte(1)) // status code

	byteSendBuffer.Put([]byte(message)) // actual body

	for groupKey, _ := range ChannelList.TCPSubscriberGroup[channelName][groupName]{

		_, err := ChannelList.TCPSubscriberGroup[channelName][groupName][groupKey].Conn.Write(byteSendBuffer.Array())

		if (err != nil){

			go ChannelList.WriteLog(err.Error())

		}

		ChannelList.TCPSubscriberGroup[channelName][groupName][groupKey].Conn.Close()

	}

	ChannelList.TCPSubscriberGroup[channelName] = make(map[string]map[int]*pojo.PacketStruct)

	go ChannelList.WriteLog(message)

}