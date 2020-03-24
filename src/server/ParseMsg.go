package server

import (
	"net"
	"time"
	"log"
	"io"
	"ChannelList"
	"pojo"
	"ByteBuffer"
	"encoding/binary"
)


func ParseMsg(packetSize int64, completePacket []byte, conn net.TCPConn, parseChan chan bool, writeCount *int, counterRequest *int, quitChannel bool){

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
			conn.Close()
			parseChan <- false
			go ChannelList.WriteLog("Invalid message received...")
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
			conn.Close()
			parseChan <- false
			go ChannelList.WriteLog("Invalid message received...")
			return
		}

		if ChannelList.TCPStorage[channelName] == nil{
			parseChan <- false
			return
		}

		currentTime := time.Now()

		nanoEpoch := currentTime.UnixNano()

		packetObject.Id = nanoEpoch
		
		if ChannelList.TCPStorage[channelName].ChannelStorageType == "persistent"{

			if *ChannelList.ConfigTCPObj.Storage.File.Active{

				go WriteData(*packetObject, writeCount)

				select {

					case message, ok := <-ChannelList.TCPStorage[channelName].WriteCallback:	
						if ok{
							
							if !message{
								parseChan <- false
								return
							}
						}		
				}
			}else{

				conn.Close()

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
			conn.Close()
			parseChan <- false
			go ChannelList.WriteLog("Invalid message received...")
			return
		}

		if ChannelList.TCPStorage[channelName] == nil{
			parseChan <- false
			return
		}

		var subscriber_offsetByte = byteBuffer.GetLong() // 8
		var subscriber_offset = binary.BigEndian.Uint64(subscriber_offsetByte)

		var startFromByte = byteBuffer.GetShort() //2
		var startFromLen = binary.BigEndian.Uint16(startFromByte)
		var start_from = string(byteBuffer.Get(int(startFromLen))) // startFromLen

		var subscriberTypeByte = byteBuffer.GetShort() //2
		var subscriberTypeLen = binary.BigEndian.Uint16(subscriberTypeByte)

		packetObject.SubscriberOffset = int64(subscriber_offset)

		packetObject.StartFromLen = int(startFromLen)

		packetObject.Start_from = start_from

		packetObject.Conn = conn

		if subscriberTypeLen > 0{

			// var groupName = string(byteBuffer.Get(int(subscriberTypeLen))) 

			// if len(ChannelList.TCPSubscriberGroup[channelName][groupName]) <= 0{

			// 	if ChannelList.TCPStorage[channelName].ChannelStorageType == "persistent"{

			// 		go SubscribeGroupChannel(channelName, groupName)

			// 	}

			// }

			// ChannelList.TCPSubscriberGroup[channelName][groupName] = append(ChannelList.TCPSubscriberGroup[channelName][groupName], packetObject)

		}else{

			if ChannelList.TCPStorage[channelName].ChannelStorageType == "persistent"{

				if !*ChannelList.ConfigTCPObj.Storage.File.Active{

					conn.Close()

					parseChan <- false
					
					return

				}

		    	if start_from != "BEGINNING" && start_from != "NOPULL" && start_from != "LASTRECEIVED"{

		    		conn.Close()

					parseChan <- false

					return
		    	}

		    	var cursor int64

		    	if start_from == "BEGINNING"{

		    		cursor = int64(0)

		    	}else if start_from == "NOPULL"{

		    		cursor = int64(-1)

		    	}else if start_from == "LASTRECEIVED"{

		    		cursor = int64(subscriber_offset)

		    	}

				go SubscribeChannel(conn, channelName, cursor, quitChannel)

			}else{

				ChannelList.TCPSocketDetails[channelName] = append(ChannelList.TCPSocketDetails[channelName], packetObject) 

			}

		}
		
		parseChan <- true 

	}else{
		conn.Close()
		parseChan <- false
		go ChannelList.WriteLog("Invalid message type must be either publish or subscribe...")
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
	
	if (err != nil && err != io.EOF ){
		go ChannelList.WriteLog(err.Error())
		ChannelList.TCPStorage[packet.ChannelName].WriteCallback <- false
		return
	}

	ChannelList.TCPStorage[packet.ChannelName].WriteCallback <- true
}
