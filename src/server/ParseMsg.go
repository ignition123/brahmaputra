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
	"os"
)

var SubscriberHashMapMtx sync.RWMutex
var SubscriberGroupMtx sync.RWMutex

func ParseMsg(packetSize int64, completePacket []byte, conn net.TCPConn, parseChan chan bool, writeCount int, counterRequest *int, subscriberMapName *string, channelMapName *string, messageMapType *string){

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

		packetObject.ActiveMode = true

		// checking for group name existence

		if ChannelList.TCPStorage[channelName].ChannelStorageType == "persistent"{

			if subscriberTypeLen > 0{

				var groupName = string(byteBuffer.Get(int(subscriberTypeLen))) 

				packetObject.GroupName = groupName

				keyFound := LoadTCPChannelSubscriberList(channelName, channelName+subscriberName+groupName)

				if keyFound{

					ThroughClientError(conn, SAME_SUBSCRIBER_DETECTED)

					parseChan <- false

					return
				   
				}

				if !*ChannelList.ConfigTCPObj.Storage.File.Active{

					ThroughClientError(conn, PERSISTENT_CONFIG_ERROR)

					parseChan <- false
					
					return

				}

				*subscriberMapName = channelName+subscriberName+groupName
				*messageMapType = messageType
    				
    			StoreTCPChannelSubscriberList(channelName, channelName+subscriberName+groupName, true)

				// checking for key already in the hashmap

				groupLen := GetChannelGrpMapLen(channelName, groupName)

				if groupLen > 0{

					if groupLen == ChannelList.TCPStorage[channelName].PartitionCount{

						ThroughClientError(conn, SUBSCRIBER_FULL)

						parseChan <- false

						return

					}

					// SetNewClientToGrp(channelName, groupName, *packetObject)

					AddNewClientToGrp(channelName, groupName, *packetObject)

				}else{

					if start_from != "BEGINNING" && start_from != "NOPULL" && start_from != "LASTRECEIVED"{

			    		ThroughClientError(conn, INVALID_PULL_FLAG)

						parseChan <- false

						return
			    	}

    				// ResetChannelGrpMap(channelName, groupName)

    				RenewSub(channelName, groupName)

    				AddNewClientToGrp(channelName, groupName, *packetObject)

    				// SetGroupMap(channelName, groupName, 0, *packetObject)

		    		go SubscribeGroupChannel(channelName, groupName, *packetObject, start_from)
				}

			}else{

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

		    	keyFound := LoadTCPChannelSubscriberList(channelName, channelName+subscriberName)

				if keyFound{

					ThroughClientError(conn, SAME_SUBSCRIBER_DETECTED)

					parseChan <- false

					return
				   
				}

    			*subscriberMapName = channelName+subscriberName
    			*messageMapType = messageType

    			StoreTCPChannelSubscriberList(channelName, channelName+subscriberName, true)

				go SubscribeChannel(conn, *packetObject, start_from)

			}

		}else{

			ChannelList.TCPSocketDetails[channelName] = append(ChannelList.TCPSocketDetails[channelName], packetObject) 
		}
		
		parseChan <- true 

	}else{

		ThroughClientError(conn, INVALID_AGENT)

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

	_, err := ChannelList.TCPStorage[packet.ChannelName].FD[writeCount].Write(byteBuffer.Array())


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

	var groupLen = len(TCPGroupStruct.TCPSubGroup[channelName][groupName])

	for i:=0;i<groupLen;i++{

		var groupObj = TCPGroupStruct.TCPSubGroup[channelName][groupName][i]

		_, err := groupObj.Conn.Write(byteSendBuffer.Array())
		
		if (err != nil){

			go ChannelList.WriteLog(err.Error())

		}

		groupObj.Conn.Close()

	}

	RenewSub(channelName, groupName)

	go ChannelList.WriteLog(message)

}

type SubscriberGroup struct{

	Index int
	Packet pojo.PacketStruct

}

func WriteSubscriberGrpOffset(index int, packetObject pojo.PacketStruct, byteArrayCursor []byte) bool{

	ChannelList.TCPStorage[packetObject.ChannelName].ChannelLock.Lock()
	_, err := packetObject.SubscriberFD[index].WriteAt(byteArrayCursor, 0)
	ChannelList.TCPStorage[packetObject.ChannelName].ChannelLock.Unlock()

	if (err != nil){

		go ChannelList.WriteLog(err.Error())

		return false
	}

	return true
}

func CloseSubscriberGrpFD(packetObject pojo.PacketStruct){

	ChannelList.TCPStorage[packetObject.ChannelName].ChannelLock.Lock()
	defer ChannelList.TCPStorage[packetObject.ChannelName].ChannelLock.Unlock()

	for fileIndex := range packetObject.SubscriberFD{

		packetObject.SubscriberFD[fileIndex].Close()

	}
}

func CreateSubscriberGrpFD(ChannelName string) []*os.File{

	ChannelList.TCPStorage[ChannelName].ChannelLock.Lock()
	var fileFDArray = make([]*os.File, ChannelList.TCPStorage[ChannelName].PartitionCount)
	ChannelList.TCPStorage[ChannelName].ChannelLock.Unlock()

	return fileFDArray

}

func AddSubscriberFD(index int, packetObject pojo.PacketStruct, fDes *os.File){

	ChannelList.TCPStorage[packetObject.ChannelName].ChannelLock.Lock()
	packetObject.SubscriberFD[index] = fDes
	ChannelList.TCPStorage[packetObject.ChannelName].ChannelLock.Unlock()

}

func LoadTCPChannelSubscriberList(channelName string, subscriberName string) bool{

	ChannelList.TCPStorage[channelName].ChannelLock.Lock()
	var status = TCPGroupStruct.TCPChannelSubscriberList[subscriberName]
	ChannelList.TCPStorage[channelName].ChannelLock.Unlock()

	return status
}	

func StoreTCPChannelSubscriberList(channelName string, subscriberName string, status bool){

	ChannelList.TCPStorage[channelName].ChannelLock.Lock()
	TCPGroupStruct.TCPChannelSubscriberList[subscriberName] = status
	ChannelList.TCPStorage[channelName].ChannelLock.Unlock()

}

func DeleteTCPChannelSubscriberList(channelName string, subscriberName string){

	ChannelList.TCPStorage[channelName].ChannelLock.Lock()
	delete(TCPGroupStruct.TCPChannelSubscriberList, subscriberName)	
	ChannelList.TCPStorage[channelName].ChannelLock.Unlock()
}



func RenewSub(channelName string, groupName string){


	ChannelList.TCPStorage[channelName].ChannelLock.Lock()
	defer ChannelList.TCPStorage[channelName].ChannelLock.Unlock()

	if TCPGroupStruct.TCPSubGroup[channelName] == nil{

		TCPGroupStruct.TCPSubGroup[channelName] = make(map[string][]*pojo.PacketStruct)
	}

	var newList []*pojo.PacketStruct

	TCPGroupStruct.TCPSubGroup[channelName][groupName] = newList

}

func AddNewClientToGrp(channelName string , groupName string, packetObject pojo.PacketStruct){

	ChannelList.TCPStorage[channelName].ChannelLock.Lock()
	TCPGroupStruct.TCPSubGroup[channelName][groupName] = append(TCPGroupStruct.TCPSubGroup[channelName][groupName], &packetObject)
	ChannelList.TCPStorage[channelName].ChannelLock.Unlock()
	
}

func RemoveGroupMember(channelName string , groupName string, consumerName string){

	ChannelList.TCPStorage[channelName].ChannelLock.Lock()
	defer ChannelList.TCPStorage[channelName].ChannelLock.Unlock()

	for index := range TCPGroupStruct.TCPSubGroup[channelName][groupName]{

		var groupMap = TCPGroupStruct.TCPSubGroup[channelName][groupName][index]

		var groupConsumerName = groupMap.ChannelName + groupMap.SubscriberName + groupMap.GroupName

		if groupConsumerName == consumerName{

			TCPGroupStruct.TCPSubGroup[channelName][groupName] = append(TCPGroupStruct.TCPSubGroup[channelName][groupName][:index], TCPGroupStruct.TCPSubGroup[channelName][groupName][index+1:]...)

			break
		}
	}

}

func RemoveGroupMap(channelName string , groupName string, groupId *int){

	ChannelList.TCPStorage[channelName].ChannelLock.Lock()
	defer ChannelList.TCPStorage[channelName].ChannelLock.Unlock()

	if len(TCPGroupStruct.TCPSubGroup[channelName][groupName]) > 0{

		TCPGroupStruct.TCPSubGroup[channelName][groupName] = append(TCPGroupStruct.TCPSubGroup[channelName][groupName][:*groupId], TCPGroupStruct.TCPSubGroup[channelName][groupName][*groupId+1:]...)
	}

}

func GetValue(channelName string , groupName string, groupId *int, index int) (*pojo.PacketStruct, int){

	ChannelList.TCPStorage[channelName].ChannelLock.Lock()
	defer ChannelList.TCPStorage[channelName].ChannelLock.Unlock()

	var groupLen = len(TCPGroupStruct.TCPSubGroup[channelName][groupName])

	if groupLen <= 0{

		return nil, -1

	}

	*groupId = index % groupLen

	if *groupId >= groupLen{

		if groupLen == 0{

			return nil, -1

		}else{

			return TCPGroupStruct.TCPSubGroup[channelName][groupName][0], 0

		}

	}else{

		return TCPGroupStruct.TCPSubGroup[channelName][groupName][*groupId], *groupId
		
	}

	return nil, -1
}

func GetChannelGrpMapLen(channelName string, groupName string) int{


	ChannelList.TCPStorage[channelName].ChannelLock.Lock()
	var groupLen = len(TCPGroupStruct.TCPSubGroup[channelName][groupName])
	ChannelList.TCPStorage[channelName].ChannelLock.Unlock()

	return groupLen
}
