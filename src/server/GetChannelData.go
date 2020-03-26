package server

import(
	"encoding/binary"
	"sync"
	"log"
	"ChannelList"
	"time"
	"net"
	"os"
	"ByteBuffer"
	"pojo"
	"strconv"
	"io/ioutil"
)

type ChannelMethods struct{
	sync.Mutex
}

func (e *ChannelMethods) GetChannelData(){

	defer ChannelList.Recover()

	for channelName := range ChannelList.TCPStorage {

	    go e.runChannel(channelName)

	    go e.subscriberChannel(channelName)

	}

}

func (e *ChannelMethods) subscriberChannel(channelName string){

	defer ChannelList.Recover()

	defer close(ChannelList.TCPStorage[channelName].SubscriberChannel)

	var cbChan = make(chan bool, 1) 

	for{

		select {

			case message, ok := <-ChannelList.TCPStorage[channelName].SubscriberChannel:	

				if ok{

					go e.sendMessageToClient(*message, cbChan)

					<-cbChan

				}

				break
		}

	}

}

func (e *ChannelMethods) runChannel(channelName string){

	defer ChannelList.Recover()

	for index := range ChannelList.TCPStorage[channelName].BucketData{

		time.Sleep(100)

		go func(index int, BucketData chan *pojo.PacketStruct, channelName string){

			defer ChannelList.Recover()

			defer close(BucketData)

			var cbChan = make(chan bool, 1)

			for{

				select {

					case message, ok := <-BucketData:	

						if ok{

							var subchannelName = message.ChannelName

							if(channelName == subchannelName && channelName != "heart_beat"){

								go e.SendAck(*message, cbChan)

								<-cbChan

								if ChannelList.TCPStorage[channelName].ChannelStorageType == "inmemory"{

									ChannelList.TCPStorage[channelName].SubscriberChannel <- message

								}
							}
						}		
					break
				}		
			}

		}(index, ChannelList.TCPStorage[channelName].BucketData[index], channelName)
	}
}

func (e *ChannelMethods) sendMessageToClient(message pojo.PacketStruct, cbChan chan bool){

	defer ChannelList.Recover()

	ackChan := make(chan bool, 1)
	subsChan := make(chan bool, 1)

	go e.SendAck(message, ackChan)

	<-ackChan

	for index := range ChannelList.TCPSocketDetails[message.ChannelName]{

		if len(ChannelList.TCPSocketDetails[message.ChannelName]) <= index{
			break
		} 

		var byteBuffer = ByteBuffer.Buffer{
			Endian:"big",
		}

		var totalByteLen = 2 + message.MessageTypeLen + 2 + message.ChannelNameLen + 2 + message.Producer_idLen + 2 + message.AgentNameLen + 8 + 8 + len(message.BodyBB)

		byteBuffer.PutLong(totalByteLen) // total packet length

		byteBuffer.PutByte(byte(0)) // status code

		byteBuffer.PutShort(message.MessageTypeLen) // total message type length

		byteBuffer.Put([]byte(message.MessageType)) // message type value

		byteBuffer.PutShort(message.ChannelNameLen) // total channel name length

		byteBuffer.Put([]byte(message.ChannelName)) // channel name value

		byteBuffer.PutShort(message.Producer_idLen) // producerid length

		byteBuffer.Put([]byte(message.Producer_id)) // producerid value

		byteBuffer.PutShort(message.AgentNameLen) // agentName length

		byteBuffer.Put([]byte(message.AgentName)) // agentName value

		byteBuffer.PutLong(int(message.Id)) // backend offset

		byteBuffer.PutLong(0) // total bytes subscriber packet received

		byteBuffer.Put(message.BodyBB) // actual body

		go e.sendInMemory(message, index, byteBuffer, subsChan)

		<-subsChan

	}

	cbChan <- true
}

func (e *ChannelMethods) sendInMemory(message pojo.PacketStruct, index int, packetBuffer ByteBuffer.Buffer, callback chan bool){ 

	defer ChannelList.Recover()

	e.Lock()

	defer e.Unlock()

	var totalRetry = 0

	RETRY:

	if len(ChannelList.TCPSocketDetails[message.ChannelName]) > index{

		totalRetry += 1

		if totalRetry > 5{

			callback <- false

			return

		}

		_, err := ChannelList.TCPSocketDetails[message.ChannelName][index].Conn.Write(packetBuffer.Array())
		
		if err != nil {
		
			go ChannelList.WriteLog(err.Error())

			var channelArray = ChannelList.TCPSocketDetails[message.ChannelName]
			copy(channelArray[index:], channelArray[index+1:])
			channelArray[len(channelArray)-1] = nil
			ChannelList.TCPSocketDetails[message.ChannelName] = channelArray[:len(channelArray)-1]

			goto RETRY

		}

		callback <- true

	}else{

		callback <- false

	}

}

func deleteKeyHashMap(consumerName string){

	SubscriberHashMapMtx.Lock()

	delete(ChannelList.TCPChannelSubscriberList, consumerName)

	SubscriberHashMapMtx.Unlock()

}

func checkCreateDirectory(conn net.TCPConn, packetObject pojo.PacketStruct, checkDirectoryChan chan bool){

	defer ChannelList.Recover()

	var consumerName = packetObject.ChannelName + packetObject.SubscriberName

	var directoryPath = ChannelList.TCPStorage[packetObject.ChannelName].Path+"/"+consumerName

	if _, err := os.Stat(directoryPath); err == nil{

		checkDirectoryChan <- true

	}else if os.IsNotExist(err){

		errDir := os.MkdirAll(directoryPath, 0755)

		if errDir != nil {
			
			ThroughClientError(conn, err.Error())

			deleteKeyHashMap(consumerName)

			checkDirectoryChan <- false

			return

		}

		go ChannelList.WriteLog("Subscriber directory created successfully...")

		checkDirectoryChan <- true

	}else{

		checkDirectoryChan <- true

	}
}

func createSubscriberOffsetFile(index int, conn net.TCPConn, packetObject pojo.PacketStruct, start_from string, partitionOffsetSubscriber chan int64){

	defer ChannelList.Recover()

	var consumerName = packetObject.ChannelName + packetObject.SubscriberName

	var directoryPath = ChannelList.TCPStorage[packetObject.ChannelName].Path+"/"+consumerName

	var consumerOffsetPath = directoryPath+"\\"+packetObject.SubscriberName+"_offset_"+strconv.Itoa(index)+".index"

	if _, err := os.Stat(consumerOffsetPath); err == nil{

		if start_from == "BEGINNING"{

			partitionOffsetSubscriber <- 0

		}else if start_from == "NOPULL"{

			partitionOffsetSubscriber <- (-1)

		}else if start_from == "LASTRECEIVED"{

			dat, err := ioutil.ReadFile(consumerOffsetPath)

			if err != nil{

				ThroughClientError(conn, err.Error())

				deleteKeyHashMap(consumerName)

				partitionOffsetSubscriber <- 0

				return

			}

			if len(dat) == 0{

				partitionOffsetSubscriber <- 0

			}else{

				partitionOffsetSubscriber <- int64(binary.BigEndian.Uint64(dat))

			}

		}else{

			partitionOffsetSubscriber <- 0

		}

		fDes, err := os.OpenFile(consumerOffsetPath,
			os.O_WRONLY, os.ModeAppend)

		if err != nil {

			ThroughClientError(conn, err.Error())

			deleteKeyHashMap(consumerName)

			partitionOffsetSubscriber <- 0

			return
		}

		packetObject.SubscriberFD[index] = fDes

	}else if os.IsNotExist(err){

		fDes, err := os.Create(consumerOffsetPath)

		if err != nil{

			ThroughClientError(conn, err.Error())

			deleteKeyHashMap(consumerName)

			partitionOffsetSubscriber <- 0

			return

		}

		packetObject.SubscriberFD[index] = fDes

		partitionOffsetSubscriber <- 0

	}else{

		if start_from == "BEGINNING"{

			partitionOffsetSubscriber <- 0

		}else if start_from == "NOPULL"{

			partitionOffsetSubscriber <- (-1)

		}else if start_from == "LASTRECEIVED"{

			dat, err := ioutil.ReadFile(consumerOffsetPath)

			if err != nil{

				ThroughClientError(conn, err.Error())

				deleteKeyHashMap(consumerName)

				partitionOffsetSubscriber <- 0

				return

			}

			if len(dat) == 0{

				partitionOffsetSubscriber <- 0

			}else{

				partitionOffsetSubscriber <- int64(binary.BigEndian.Uint64(dat))

			}

		}else{

			partitionOffsetSubscriber <- 0

		}

	}

}

func checkCreateGroupDirectory(channelName string, groupName string, checkDirectoryChan chan bool){

	defer ChannelList.Recover()

	var directoryPath = ChannelList.TCPStorage[channelName].Path+"/"+groupName

	if _, err := os.Stat(directoryPath); err == nil{

		checkDirectoryChan <- true

	}else if os.IsNotExist(err){

		errDir := os.MkdirAll(directoryPath, 0755)

		if errDir != nil {
			
			ThroughGroupError(channelName, groupName, err.Error())

			checkDirectoryChan <- false

			return

		}

		go ChannelList.WriteLog("Subscriber directory created successfully...")

		checkDirectoryChan <- true

	}else{

		checkDirectoryChan <- true

	}

}

func createSubscriberGroupOffsetFile(index int, channelName string, groupName string, packetObject pojo.PacketStruct, partitionOffsetSubscriber chan int64, start_from string){

	defer ChannelList.Recover()

	var directoryPath = ChannelList.TCPStorage[channelName].Path+"/"+groupName

	var consumerOffsetPath = directoryPath+"\\"+groupName+"_offset_"+strconv.Itoa(index)+".index"

	if _, err := os.Stat(consumerOffsetPath); err == nil{

		log.Println(start_from)

		if start_from == "BEGINNING"{

			partitionOffsetSubscriber <- 0

		}else if start_from == "NOPULL"{

			partitionOffsetSubscriber <- (-1)

		}else if start_from == "LASTRECEIVED"{

			dat, err := ioutil.ReadFile(consumerOffsetPath)

			if err != nil{

				ThroughGroupError(channelName, groupName, err.Error())

				partitionOffsetSubscriber <- 0

				return

			}

			if len(dat) == 0{

				partitionOffsetSubscriber <- 0

			}else{

				partitionOffsetSubscriber <- int64(binary.BigEndian.Uint64(dat))

			}

		}else{

			partitionOffsetSubscriber <- 0

		}

		fDes, err := os.OpenFile(consumerOffsetPath,
			os.O_WRONLY, os.ModeAppend)

		if err != nil {

			ThroughGroupError(channelName, groupName, err.Error())

			partitionOffsetSubscriber <- 0

			return
		}

		packetObject.SubscriberFD[index] = fDes

	}else if os.IsNotExist(err){

		fDes, err := os.Create(consumerOffsetPath)

		if err != nil{

			ThroughGroupError(channelName, groupName, err.Error())

			partitionOffsetSubscriber <- 0

			return

		}

		packetObject.SubscriberFD[index] = fDes

		partitionOffsetSubscriber <- 0

	}else{

		if start_from == "BEGINNING"{

			partitionOffsetSubscriber <- 0

		}else if start_from == "NOPULL"{

			partitionOffsetSubscriber <- (-1)

		}else if start_from == "LASTRECEIVED"{

			dat, err := ioutil.ReadFile(consumerOffsetPath)

			if err != nil{

				ThroughGroupError(channelName, groupName, err.Error())

				partitionOffsetSubscriber <- 0

				return

			}

			if len(dat) == 0{

				partitionOffsetSubscriber <- 0

			}else{

				partitionOffsetSubscriber <- int64(binary.BigEndian.Uint64(dat))

			}

		}else{

			partitionOffsetSubscriber <- 0

		}

	}

}


func SubscribeGroupChannel(channelName string, groupName string, packetObject pojo.PacketStruct, start_from string){

	defer ChannelList.Recover()

	var groupMtx sync.Mutex

	var checkDirectoryChan = make(chan bool)

	var offsetByteSize = make([]int64, ChannelList.TCPStorage[packetObject.ChannelName].PartitionCount)

	var partitionOffsetSubscriber = make(chan int64)

	go checkCreateGroupDirectory(channelName, groupName, checkDirectoryChan)

	if false == <-checkDirectoryChan{

		return

	}

	packetObject.SubscriberFD = make([]*os.File, ChannelList.TCPStorage[packetObject.ChannelName].PartitionCount)

	for i:=0;i<ChannelList.TCPStorage[packetObject.ChannelName].PartitionCount;i++{

		go createSubscriberGroupOffsetFile(i, channelName, groupName, packetObject, partitionOffsetSubscriber, start_from)

		offsetByteSize[i] = <-partitionOffsetSubscriber

	}

	if len(packetObject.SubscriberFD) == 0{

		ThroughGroupError(channelName, groupName, INVALID_SUBSCRIBER_OFFSET)

		return

	}

	var groupId int

	for i:=0;i<ChannelList.TCPStorage[packetObject.ChannelName].PartitionCount;i++{

		go func(index int, cursor int64, packetObject pojo.PacketStruct){

			defer ChannelList.Recover()

			var filePath = ChannelList.TCPStorage[packetObject.ChannelName].Path+"/"+packetObject.ChannelName+"_partition_"+strconv.Itoa(index)+".br"

			file, err := os.Open(filePath)

			defer file.Close()

			if err != nil {

				ThroughGroupError(packetObject.ChannelName, packetObject.GroupName, err.Error())

				return
			}

			var sentMsg = make(chan bool, 1)

			defer close(sentMsg)

			var exitLoop = false

			for{

				if exitLoop{

					log.Println("all killed")

					if len(ChannelList.TCPSubscriberGroup[packetObject.ChannelName][packetObject.GroupName]) <= 0{

						break

					}

				}

				fileStat, err := os.Stat(filePath)
		 
				if err != nil {

					ThroughGroupError(packetObject.ChannelName, packetObject.GroupName, err.Error())

					break
					
				}

				if cursor == -1{

					cursor = fileStat.Size()

				}


				if cursor < fileStat.Size(){

					data := make([]byte, 8)

					count, err := file.ReadAt(data, cursor)

					if err != nil {

						continue

					}

					if count > 0{

						cursor += 8

						var packetSize = binary.BigEndian.Uint64(data)

						restPacket := make([]byte, packetSize)

						totalByteLen, errPacket := file.ReadAt(restPacket, cursor)

						if errPacket != nil{

							continue

						}

						if totalByteLen > 0{

							cursor += int64(packetSize)

							var byteFileBuffer = ByteBuffer.Buffer{
								Endian:"big",
							}

							byteFileBuffer.Wrap(restPacket)

							var messageTypeByte = byteFileBuffer.GetShort()
							var messageTypeLen = int(binary.BigEndian.Uint16(messageTypeByte))
							var messageType = byteFileBuffer.Get(messageTypeLen)

							var channelNameByte = byteFileBuffer.GetShort()
							var channelNameLen = int(binary.BigEndian.Uint16(channelNameByte))
							var channelName = byteFileBuffer.Get(channelNameLen)

							var producer_idByte = byteFileBuffer.GetShort()
							var producer_idLen = int(binary.BigEndian.Uint16(producer_idByte))
							var producer_id = byteFileBuffer.Get(producer_idLen)

							var agentNameByte  = byteFileBuffer.GetShort()
							var agentNameLen = int(binary.BigEndian.Uint16(agentNameByte))
							var agentName = byteFileBuffer.Get(agentNameLen)

							var idByte = byteFileBuffer.GetLong()
							var id = binary.BigEndian.Uint64(idByte)

							var bodyPacketSize = int64(packetSize) - int64(2 + messageTypeLen + 2 + channelNameLen + 2 + producer_idLen + 2 + agentNameLen + 8)

							var bodyBB = byteFileBuffer.Get(int(bodyPacketSize))

							var newTotalByteLen = 2 + messageTypeLen + 2 + channelNameLen + 2 + producer_idLen + 2 + agentNameLen + 8 + len(bodyBB)

							var byteSendBuffer = ByteBuffer.Buffer{
								Endian:"big",
							}

							byteSendBuffer.PutLong(newTotalByteLen) // total packet length

							byteSendBuffer.PutByte(byte(0)) // status code

							byteSendBuffer.PutShort(messageTypeLen) // total message type length

							byteSendBuffer.Put([]byte(messageType)) // message type value

							byteSendBuffer.PutShort(channelNameLen) // total channel name length

							byteSendBuffer.Put([]byte(channelName)) // channel name value

							byteSendBuffer.PutShort(producer_idLen) // producerid length

							byteSendBuffer.Put([]byte(producer_id)) // producerid value

							byteSendBuffer.PutShort(agentNameLen) // agentName length

							byteSendBuffer.Put([]byte(agentName)) // agentName value

							byteSendBuffer.PutLong(int(id)) // backend offset

							byteSendBuffer.Put(bodyBB) // actual body

							var groupLen  = len(ChannelList.TCPSubscriberGroup[packetObject.ChannelName][packetObject.GroupName])

							if groupLen <= 0{

								break
							}

							groupId = index % groupLen

							go sendGroup(index, &groupId, groupLen, groupMtx, int(cursor), packetObject, byteSendBuffer, sentMsg)

							select{
								case message, ok := <-sentMsg:

									if ok{

										if !message{

											exitLoop = true

										}else{

											exitLoop = false

										}

									}

									break
							}

						}else{

							time.Sleep(1 * time.Second)

						}

					}else{

						time.Sleep(1 * time.Second)
					}

				}else{

					cursor = fileStat.Size()

					time.Sleep(1 * time.Second)
				}

			}

		}(i, offsetByteSize[i], packetObject)

	}

}

func SubscribeChannel(conn net.TCPConn, packetObject pojo.PacketStruct, start_from string){

	defer ChannelList.Recover()

	var consumerName = packetObject.ChannelName + packetObject.SubscriberName

	var checkDirectoryChan = make(chan bool)

	var offsetByteSize = make([]int64, ChannelList.TCPStorage[packetObject.ChannelName].PartitionCount)

	var partitionOffsetSubscriber = make(chan int64)

	go checkCreateDirectory(conn, packetObject, checkDirectoryChan)

	if false == <-checkDirectoryChan{

		return

	}

	packetObject.SubscriberFD = make([]*os.File, ChannelList.TCPStorage[packetObject.ChannelName].PartitionCount)

	for i:=0;i<ChannelList.TCPStorage[packetObject.ChannelName].PartitionCount;i++{

		go createSubscriberOffsetFile(i, conn, packetObject, start_from, partitionOffsetSubscriber)

		offsetByteSize[i] = <-partitionOffsetSubscriber

	}

	if len(packetObject.SubscriberFD) == 0{

		ThroughClientError(conn, INVALID_SUBSCRIBER_OFFSET)

		deleteKeyHashMap(consumerName)

		return

	}

	for i:=0;i<ChannelList.TCPStorage[packetObject.ChannelName].PartitionCount;i++{

		go func(index int, cursor int64, conn net.TCPConn, packetObject pojo.PacketStruct){

			defer ChannelList.Recover()

			var filePath = ChannelList.TCPStorage[packetObject.ChannelName].Path+"/"+packetObject.ChannelName+"_partition_"+strconv.Itoa(index)+".br"

			file, err := os.Open(filePath)

			defer file.Close()

			if err != nil {
				
				ThroughClientError(conn, err.Error())

				deleteKeyHashMap(consumerName)

				return
			}

			var sentMsg = make(chan bool, 1)

			defer close(sentMsg)

			var exitLoop = false

			for{

				_, keyFound := ChannelList.TCPChannelSubscriberList[consumerName]

				if exitLoop || !keyFound{

					conn.Close()

					for fileIndex := range packetObject.SubscriberFD{

						packetObject.SubscriberFD[fileIndex].Close()

					}

					deleteKeyHashMap(consumerName)

					break
				}

				fileStat, err := os.Stat(filePath)
		 
				if err != nil {

					ThroughClientError(conn, err.Error())

					deleteKeyHashMap(consumerName)

					break
					
				}

				if cursor == -1{

					cursor = fileStat.Size()

				}

				if cursor < fileStat.Size(){

					data := make([]byte, 8)

					count, err := file.ReadAt(data, cursor)

					if err != nil {

						continue

					}

					if count > 0{

						cursor += 8

						var packetSize = binary.BigEndian.Uint64(data)

						restPacket := make([]byte, packetSize)

						totalByteLen, errPacket := file.ReadAt(restPacket, cursor)

						if errPacket != nil{

							continue

						}

						if totalByteLen > 0{

							cursor += int64(packetSize)

							var byteFileBuffer = ByteBuffer.Buffer{
								Endian:"big",
							}

							byteFileBuffer.Wrap(restPacket)

							var messageTypeByte = byteFileBuffer.GetShort()
							var messageTypeLen = int(binary.BigEndian.Uint16(messageTypeByte))
							var messageType = byteFileBuffer.Get(messageTypeLen)

							var channelNameByte = byteFileBuffer.GetShort()
							var channelNameLen = int(binary.BigEndian.Uint16(channelNameByte))
							var channelName = byteFileBuffer.Get(channelNameLen)

							var producer_idByte = byteFileBuffer.GetShort()
							var producer_idLen = int(binary.BigEndian.Uint16(producer_idByte))
							var producer_id = byteFileBuffer.Get(producer_idLen)

							var agentNameByte  = byteFileBuffer.GetShort()
							var agentNameLen = int(binary.BigEndian.Uint16(agentNameByte))
							var agentName = byteFileBuffer.Get(agentNameLen)

							var idByte = byteFileBuffer.GetLong()
							var id = binary.BigEndian.Uint64(idByte)

							var bodyPacketSize = int64(packetSize) - int64(2 + messageTypeLen + 2 + channelNameLen + 2 + producer_idLen + 2 + agentNameLen + 8)

							var bodyBB = byteFileBuffer.Get(int(bodyPacketSize))

							var newTotalByteLen = 2 + messageTypeLen + 2 + channelNameLen + 2 + producer_idLen + 2 + agentNameLen + 8 + len(bodyBB)

							var byteSendBuffer = ByteBuffer.Buffer{
								Endian:"big",
							}

							byteSendBuffer.PutLong(newTotalByteLen) // total packet length

							byteSendBuffer.PutByte(byte(0)) // status code

							byteSendBuffer.PutShort(messageTypeLen) // total message type length

							byteSendBuffer.Put([]byte(messageType)) // message type value

							byteSendBuffer.PutShort(channelNameLen) // total channel name length

							byteSendBuffer.Put([]byte(channelName)) // channel name value

							byteSendBuffer.PutShort(producer_idLen) // producerid length

							byteSendBuffer.Put([]byte(producer_id)) // producerid value

							byteSendBuffer.PutShort(agentNameLen) // agentName length

							byteSendBuffer.Put([]byte(agentName)) // agentName value

							byteSendBuffer.PutLong(int(id)) // backend offset

							byteSendBuffer.Put(bodyBB) // actual body

							go send(index, int(cursor), packetObject, conn, byteSendBuffer, sentMsg)

							select{
								case message, ok := <-sentMsg:

									if ok{

										if !message{

											exitLoop = true

										}else{

											exitLoop = false

										}

									}

									break
							}

						}else{

							time.Sleep(1 * time.Second)

						}

					}else{

						time.Sleep(1 * time.Second)
					}

				}else{

					cursor = fileStat.Size()

					time.Sleep(1 * time.Second)
				}
			}


		}(i, offsetByteSize[i], conn, packetObject)

	}

}

func sendGroup(index int, groupId *int, groupLen int, groupMtx sync.Mutex, cursor int, packetObject pojo.PacketStruct, packetBuffer ByteBuffer.Buffer, sentMsg chan bool){

	defer ChannelList.Recover()

	groupMtx.Lock()

	defer groupMtx.Unlock()

	RETRY:

	if *groupId < 0{

		sentMsg <- false

		return
	}

	var group = ChannelList.TCPSubscriberGroup[packetObject.ChannelName][packetObject.GroupName][*groupId]

	log.Println(index)

	_, err := group.Conn.Write(packetBuffer.Array())
	
	if err != nil {

		log.Println("killed")

		ChannelList.TCPSubscriberGroup[packetObject.ChannelName][packetObject.GroupName][*groupId] = ChannelList.TCPSubscriberGroup[packetObject.ChannelName][packetObject.GroupName][len(ChannelList.TCPSubscriberGroup[packetObject.ChannelName][packetObject.GroupName])-1]
		ChannelList.TCPSubscriberGroup[packetObject.ChannelName][packetObject.GroupName][len(ChannelList.TCPSubscriberGroup[packetObject.ChannelName][packetObject.GroupName])-1] = nil
		ChannelList.TCPSubscriberGroup[packetObject.ChannelName][packetObject.GroupName] = ChannelList.TCPSubscriberGroup[packetObject.ChannelName][packetObject.GroupName][:len(ChannelList.TCPSubscriberGroup[packetObject.ChannelName][packetObject.GroupName])-1] 

		*groupId  = groupLen - 1

		goto RETRY
	}

	byteArrayCursor := make([]byte, 8)
	binary.BigEndian.PutUint64(byteArrayCursor, uint64(cursor))

	_, err = packetObject.SubscriberFD[index].WriteAt(byteArrayCursor, 0)

	if (err != nil){

		go ChannelList.WriteLog(err.Error())

		sentMsg <- false

		return
	}

	sentMsg <- true
}

func send(index int, cursor int, packetObject pojo.PacketStruct, conn net.TCPConn, packetBuffer ByteBuffer.Buffer, sentMsg chan bool){ 

	defer ChannelList.Recover()

	_, err := conn.Write(packetBuffer.Array())
	
	if err != nil {
	
		go ChannelList.WriteLog(err.Error())

		sentMsg <- false

		return
	}

	byteArrayCursor := make([]byte, 8)
	binary.BigEndian.PutUint64(byteArrayCursor, uint64(cursor))

	_, err = packetObject.SubscriberFD[index].WriteAt(byteArrayCursor, 0)

	if (err != nil){

		go ChannelList.WriteLog(err.Error())

		sentMsg <- false

		return
	}

	sentMsg <- true

}

func (e *ChannelMethods) SendAck(messageMap pojo.PacketStruct, ackChan chan bool){

	defer ChannelList.Recover()

	var byteBuffer = ByteBuffer.Buffer{
		Endian:"big",
	}

	byteBuffer.PutLong(len(messageMap.Producer_id))

	byteBuffer.PutByte(byte(0)) // status code

	byteBuffer.Put([]byte(messageMap.Producer_id))

	_, err := messageMap.Conn.Write(byteBuffer.Array())

	if err != nil{

		go ChannelList.WriteLog(err.Error())

		ackChan <- false

		return
	}

	ackChan <- true

}