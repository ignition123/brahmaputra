package tcp

import(
	"encoding/binary"
	"sync"
	_"log"
	"ChannelList"
	"time"
	"net"
	"os"
	"ByteBuffer"
	"pojo"
	"strconv"
	"io/ioutil"
)

var ChannelMethod = &ChannelMethods{}

type ChannelMethods struct{
	sync.Mutex
}

func (e *ChannelMethods) GetChannelData(){

	defer ChannelList.Recover()

	for channelName := range ChannelList.TCPStorage {

	    go e.runChannel(channelName)
	}
}


func (e *ChannelMethods) runChannel(channelName string){

	defer ChannelList.Recover()

	for index := range ChannelList.TCPStorage[channelName].BucketData{

		time.Sleep(100)

		go func(index int, BucketData chan *pojo.PacketStruct, channelName string){

			defer ChannelList.Recover()

			defer close(BucketData)

			var msgChan = make(chan bool, 1)
			defer close(msgChan)

			for{

				select {

					case message, ok := <-BucketData:	

						if ok{

							var subchannelName = message.ChannelName

							if(channelName == subchannelName && channelName != "heart_beat"){

								go e.sendMessageToClient(*message, msgChan)

								<-msgChan

							}
						}		
					break
				}		
			}

		}(index, ChannelList.TCPStorage[channelName].BucketData[index], channelName)
	}
}

func (e *ChannelMethods) sendMessageToClient(message pojo.PacketStruct, msgChan chan bool){

	defer ChannelList.Recover()

	var channelSockList = GetClientListInmemory(message.ChannelName)

	for key, _ := range channelSockList{

		var byteBuffer = ByteBuffer.Buffer{
			Endian:"big",
		}

		var totalByteLen = 2 + message.MessageTypeLen + 2 + message.ChannelNameLen + 2 + message.Producer_idLen + 2 + message.AgentNameLen + 8 + 8 + len(message.BodyBB)

		byteBuffer.PutLong(totalByteLen) // total packet length

		byteBuffer.PutByte(byte(2)) // status code

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

		e.sendInMemory(message, key, byteBuffer)
	}

	msgChan <- true
}

func (e *ChannelMethods) sendInMemory(message pojo.PacketStruct, key string, packetBuffer ByteBuffer.Buffer){ 

	defer ChannelList.Recover()

	var stat, sock = FindInmemorySocketListLength(message.ChannelName, key)

	if stat{

		_, err := sock.Conn.Write(packetBuffer.Array())
		
		if err != nil {
		
			go ChannelList.WriteLog(err.Error())

			return

		}

	}
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

			DeleteTCPChannelSubscriberList(packetObject.ChannelName, consumerName)

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

				DeleteTCPChannelSubscriberList(packetObject.ChannelName, consumerName)

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

			DeleteTCPChannelSubscriberList(packetObject.ChannelName, consumerName)

			partitionOffsetSubscriber <- 0

			return
		}

		AddSubscriberFD(index, packetObject, fDes)

	}else if os.IsNotExist(err){

		fDes, err := os.Create(consumerOffsetPath)

		if err != nil{

			ThroughClientError(conn, err.Error())

			DeleteTCPChannelSubscriberList(packetObject.ChannelName, consumerName)

			partitionOffsetSubscriber <- 0

			return

		}

		AddSubscriberFD(index, packetObject, fDes)

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

				DeleteTCPChannelSubscriberList(packetObject.ChannelName, consumerName)

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
			os.O_WRONLY, os.ModeAppend) //race

		if err != nil {

			ThroughGroupError(channelName, groupName, err.Error())

			partitionOffsetSubscriber <- 0

			return
		}

		AddSubscriberFD(index, packetObject, fDes) // race

	}else if os.IsNotExist(err){

		fDes, err := os.Create(consumerOffsetPath)

		if err != nil{

			ThroughGroupError(channelName, groupName, err.Error())

			partitionOffsetSubscriber <- 0

			return

		}

		AddSubscriberFD(index, packetObject, fDes)

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

	var checkDirectoryChan = make(chan bool, 1)
	defer close(checkDirectoryChan)

	var offsetByteSize = make([]int64, ChannelList.TCPStorage[packetObject.ChannelName].PartitionCount)

	var partitionOffsetSubscriber = make(chan int64, 1)
	defer close(partitionOffsetSubscriber)

	go checkCreateGroupDirectory(channelName, groupName, checkDirectoryChan)

	if false == <-checkDirectoryChan{

		return

	}

	packetObject.SubscriberFD = CreateSubscriberGrpFD(packetObject.ChannelName)

	for i:=0;i<ChannelList.TCPStorage[packetObject.ChannelName].PartitionCount;i++{

		go createSubscriberGroupOffsetFile(i, channelName, groupName, packetObject, partitionOffsetSubscriber, start_from) // race

		offsetByteSize[i] = <-partitionOffsetSubscriber

	}

	if len(packetObject.SubscriberFD) == 0{

		ThroughGroupError(channelName, groupName, INVALID_SUBSCRIBER_OFFSET)

		return

	}

	for i:=0;i<ChannelList.TCPStorage[packetObject.ChannelName].PartitionCount;i++{

		go func(index int, cursor int64, packetObject pojo.PacketStruct){

			defer ChannelList.Recover()

			var groupMtx sync.Mutex

			var filePath = ChannelList.TCPStorage[packetObject.ChannelName].Path+"/"+packetObject.ChannelName+"_partition_"+strconv.Itoa(index)+".br"

			file, err := os.Open(filePath)

			if err != nil {

				ThroughGroupError(packetObject.ChannelName, packetObject.GroupName, err.Error())

				return
			}

			defer file.Close()

			var sentMsg = make(chan bool, 1)
			defer close(sentMsg)

			var exitLoop = false

			for{

				if exitLoop{

					var consumerGroupLen = GetChannelGrpMapLen(packetObject.ChannelName, packetObject.GroupName)

					if consumerGroupLen <= 0{

						go CloseSubscriberGrpFD(packetObject)

					}

					break

				}

				fileStat, err := os.Stat(filePath)
		 
				if err != nil {

					ThroughGroupError(packetObject.ChannelName, packetObject.GroupName, err.Error())

					exitLoop = true
					
					continue
					
				}

				if cursor == -1{

					cursor = fileStat.Size()

				}

				if cursor >= fileStat.Size(){

					time.Sleep(1 * time.Second)

					continue

				}


				data := make([]byte, 8)

				count, err := file.ReadAt(data, cursor)

				if err != nil {

					time.Sleep(1 * time.Second)

					continue

				}

				var packetSize = binary.BigEndian.Uint64(data)

				if int64(count) <= 0 || int64(packetSize) >= fileStat.Size(){

					time.Sleep(1 * time.Second)

					continue

				}


				cursor += int64(8)

				restPacket := make([]byte, int64(packetSize))

				totalByteLen, errPacket := file.ReadAt(restPacket, cursor)

				if errPacket != nil{

					time.Sleep(1 * time.Second)

					continue

				}

				if totalByteLen <= 0{

					time.Sleep(1 * time.Second)

					continue
				}


				cursor += int64(packetSize)

				byteFileBuffer := ByteBuffer.Buffer{
					Endian:"big",
				}

				byteFileBuffer.Wrap(restPacket)

				messageTypeLen := int(binary.BigEndian.Uint16(byteFileBuffer.GetShort()))
				messageType := byteFileBuffer.Get(messageTypeLen)

				channelNameLen := int(binary.BigEndian.Uint16(byteFileBuffer.GetShort()))
				channelName := byteFileBuffer.Get(channelNameLen)

				producer_idLen := int(binary.BigEndian.Uint16(byteFileBuffer.GetShort()))
				producer_id := byteFileBuffer.Get(producer_idLen)

				agentNameLen := int(binary.BigEndian.Uint16(byteFileBuffer.GetShort()))
				agentName := byteFileBuffer.Get(agentNameLen)

				id := binary.BigEndian.Uint64(byteFileBuffer.GetLong())

				bodyPacketSize := int64(packetSize) - int64(2 + messageTypeLen + 2 + channelNameLen + 2 + producer_idLen + 2 + agentNameLen + 8)

				bodyBB := byteFileBuffer.Get(int(bodyPacketSize))

				newTotalByteLen := 2 + messageTypeLen + 2 + channelNameLen + 2 + producer_idLen + 2 + agentNameLen + 8 + len(bodyBB)

				byteSendBuffer := ByteBuffer.Buffer{
					Endian:"big",
				}

				byteSendBuffer.PutLong(newTotalByteLen) // total packet length

				byteSendBuffer.PutByte(byte(2)) // status code

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

				// log.Println(ChannelList.TCPStorage[packetObject.ChannelName][packetObject.GroupName])

				go sendGroup(index, groupMtx, int(cursor), packetObject, byteSendBuffer, sentMsg) // race

				message, ok := <-sentMsg

				if ok{

					if !message{

						exitLoop = true


					}else{

						exitLoop = false

					}
				}

			}

		}(i, offsetByteSize[i], packetObject)

	}

}

func SubscribeChannel(conn net.TCPConn, packetObject pojo.PacketStruct, start_from string){

	defer ChannelList.Recover()

	var consumerName = packetObject.ChannelName + packetObject.SubscriberName

	var checkDirectoryChan = make(chan bool, 1)
	defer close(checkDirectoryChan)

	var offsetByteSize = make([]int64, ChannelList.TCPStorage[packetObject.ChannelName].PartitionCount)

	var partitionOffsetSubscriber = make(chan int64, 1)
	defer close(partitionOffsetSubscriber)

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

		DeleteTCPChannelSubscriberList(packetObject.ChannelName, consumerName)

		return

	}

	var subscriberMtx sync.Mutex

	for i:=0;i<ChannelList.TCPStorage[packetObject.ChannelName].PartitionCount;i++{

		go func(index int, cursor int64, conn net.TCPConn, packetObject pojo.PacketStruct){

			defer ChannelList.Recover()

			var filePath = ChannelList.TCPStorage[packetObject.ChannelName].Path+"/"+packetObject.ChannelName+"_partition_"+strconv.Itoa(index)+".br"

			file, err := os.Open(filePath)

			defer file.Close()

			if err != nil {
				
				ThroughClientError(conn, err.Error())

				DeleteTCPChannelSubscriberList(packetObject.ChannelName, consumerName)

				return
			}

			var sentMsg = make(chan bool, 1)
			defer close(sentMsg)

			var exitLoop = false

			for{

				if exitLoop{

					conn.Close()

					go CloseSubscriberGrpFD(packetObject)

					DeleteTCPChannelSubscriberList(packetObject.ChannelName, consumerName)

					break

				}

				fileStat, err := os.Stat(filePath)
		 
				if err != nil {

					ThroughClientError(conn, err.Error())

					DeleteTCPChannelSubscriberList(packetObject.ChannelName, consumerName)

					break
					
				}

				if cursor == -1{

					cursor = fileStat.Size()

				}

				if cursor >= fileStat.Size(){

					time.Sleep(1 * time.Second)

					continue

				}


				data := make([]byte, 8)

				count, err := file.ReadAt(data, cursor)

				if err != nil {

					time.Sleep(1 * time.Second)

					continue

				}

				var packetSize = binary.BigEndian.Uint64(data)

				if int64(count) <= 0 || int64(packetSize) >= fileStat.Size(){

					time.Sleep(1 * time.Second)

					continue

				}

				cursor += int64(8)

				restPacket := make([]byte, packetSize)

				totalByteLen, errPacket := file.ReadAt(restPacket, cursor)

				if errPacket != nil{

					time.Sleep(1 * time.Second)

					continue

				}

				if totalByteLen <= 0{

					time.Sleep(1 * time.Second)

					continue

				}



				cursor += int64(packetSize)

				byteFileBuffer := ByteBuffer.Buffer{
					Endian:"big",
				}

				byteFileBuffer.Wrap(restPacket)

				messageTypeLen := int(binary.BigEndian.Uint16(byteFileBuffer.GetShort()))
				messageType := byteFileBuffer.Get(messageTypeLen)

				channelNameLen := int(binary.BigEndian.Uint16(byteFileBuffer.GetShort()))
				channelName := byteFileBuffer.Get(channelNameLen)

				producer_idLen := int(binary.BigEndian.Uint16(byteFileBuffer.GetShort()))
				producer_id := byteFileBuffer.Get(producer_idLen)

				agentNameLen := int(binary.BigEndian.Uint16(byteFileBuffer.GetShort()))
				agentName := byteFileBuffer.Get(agentNameLen)

				id := binary.BigEndian.Uint64(byteFileBuffer.GetLong())

				bodyPacketSize := int64(packetSize) - int64(2 + messageTypeLen + 2 + channelNameLen + 2 + producer_idLen + 2 + agentNameLen + 8)

				bodyBB := byteFileBuffer.Get(int(bodyPacketSize))

				newTotalByteLen := 2 + messageTypeLen + 2 + channelNameLen + 2 + producer_idLen + 2 + agentNameLen + 8 + len(bodyBB)

				byteSendBuffer := ByteBuffer.Buffer{
					Endian:"big",
				}

				byteSendBuffer.PutLong(newTotalByteLen) // total packet length

				byteSendBuffer.PutByte(byte(2)) // status code

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

				go send(index, int(cursor), subscriberMtx, packetObject, conn, byteSendBuffer, sentMsg)

				message, ok := <-sentMsg

				if ok{

					if !message{

						exitLoop = true

					}else{

						exitLoop = false

					}

				}

			}


		}(i, offsetByteSize[i], conn, packetObject)

	}

}

func sendGroup(index int, groupMtx sync.Mutex, cursor int, packetObject pojo.PacketStruct, packetBuffer ByteBuffer.Buffer, sentMsg chan bool){

	defer ChannelList.Recover()

	groupMtx.Lock()

	defer groupMtx.Unlock()

	var groupId = 0

	var group *pojo.PacketStruct

	RETRY:

	group, groupId = GetValue(packetObject.ChannelName, packetObject.GroupName, &groupId, index)

	if group == nil{

		sentMsg <- false

		return
	}
	
	_, err := group.Conn.Write(packetBuffer.Array())
	
	if err != nil {

		group, groupId = GetValue(packetObject.ChannelName, packetObject.GroupName, &groupId, index)

		if group == nil{

			sentMsg <- false

			return
		}

		goto RETRY

	}

	byteArrayCursor := make([]byte, 8)
	binary.BigEndian.PutUint64(byteArrayCursor, uint64(cursor))

	sentMsg <- WriteSubscriberGrpOffset(index, packetObject, byteArrayCursor)
}

func send(index int, cursor int, subscriberMtx sync.Mutex, packetObject pojo.PacketStruct, conn net.TCPConn, packetBuffer ByteBuffer.Buffer, sentMsg chan bool){ 

	defer ChannelList.Recover()

	subscriberMtx.Lock()
	defer subscriberMtx.Unlock()

	_, err := conn.Write(packetBuffer.Array())
	
	if err != nil {
	
		go ChannelList.WriteLog(err.Error())

		sentMsg <- false

		return
	}

	byteArrayCursor := make([]byte, 8)
	binary.BigEndian.PutUint64(byteArrayCursor, uint64(cursor))

	sentMsg <- WriteSubscriberGrpOffset(index, packetObject, byteArrayCursor)

}

func (e *ChannelMethods) SendAck(messageMap pojo.PacketStruct, ackChan chan bool){

	defer ChannelList.Recover()

	var byteBuffer = ByteBuffer.Buffer{
		Endian:"big",
	}

	byteBuffer.PutLong(len(messageMap.Producer_id))

	byteBuffer.PutByte(byte(2)) // status code

	byteBuffer.Put([]byte(messageMap.Producer_id))

	_, err := messageMap.Conn.Write(byteBuffer.Array())

	if err != nil{

		go ChannelList.WriteLog(err.Error())

		ackChan <- false

		return
	}

	ackChan <- true

}