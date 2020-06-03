package tcp

/*
	This file contains all publishing from server to client methods

	It contains both inmemory as well as persistent streaming methods

	original @author Sudeep Dasgupta
*/

// importing modules

import(
	"encoding/binary"
	"sync"
	"ChannelList"
	"time"
	"net"
	"os"
	"ByteBuffer"
	"pojo"
	"strconv"
	"io/ioutil"
)

// declaring a struct with a mutex

type ChannelMethods struct{
	sync.Mutex
}

// initializing a structure globally

var ChannelMethod = &ChannelMethods{}

// creating channels for inmemory subscription

func (e *ChannelMethods) GetChannelData(){

	defer ChannelList.Recover()

	for channelName := range ChannelList.TCPStorage {

	    go e.runChannel(channelName)
	}
}

// initializing channels

func (e *ChannelMethods) runChannel(channelName string){

	defer ChannelList.Recover()

	// iterating over the channel bucket and initializing

	for index := range ChannelList.TCPStorage[channelName].BucketData{

		time.Sleep(100)

		// using go routines for starting infinite loops

		go func(index int, BucketData chan *pojo.PacketStruct, channelName string){

			defer ChannelList.Recover()
			defer close(BucketData)

			msgChan := make(chan bool, 1)
			defer close(msgChan)

			// infinitely listening to channels

			for{

				select {

					case message, ok := <-BucketData:	

						if ok{

							// if messages arives then

							subchannelName := message.ChannelName

							// checking for not heart_beat channels

							if(channelName == subchannelName && channelName != "heart_beat"){

								// publishing to all subsriber

								go e.sendMessageToClient(*message, msgChan)

								// waiting for channel callback

								<-msgChan

							}
						}		
					break
				}		
			}

		}(index, ChannelList.TCPStorage[channelName].BucketData[index], channelName)
	}
}

// sending to all clients that are connected to inmemory channels

func (e *ChannelMethods) sendMessageToClient(message pojo.PacketStruct, msgChan chan bool){

	defer ChannelList.Recover()

	// getting the client list this method uses mutex to prevent race condition

	channelSockList := GetClientListInmemory(message.ChannelName)

	// iterating over the hashmap

	for key, _ := range channelSockList{

		// creating bytebuffer

		byteBuffer := ByteBuffer.Buffer{
			Endian:"big",
		}

		// creating total length of the byte buffer

		// MessageTypeLen + messageType + ChannelNameLength + channelname + producer_idLen + producer_id + AgentNameLen + AgentName + backendOffset + actualBody

		totalByteLen := 2 + message.MessageTypeLen + 2 + message.ChannelNameLen + 2 + message.Producer_idLen + 2 + message.AgentNameLen + 8 + 8 + len(message.BodyBB)

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

// sending Inmemory client

func (e *ChannelMethods) sendInMemory(message pojo.PacketStruct, key string, packetBuffer ByteBuffer.Buffer){ 

	defer ChannelList.Recover()

	// getting the length of the socket client connected

	stat, sock := FindInmemorySocketListLength(message.ChannelName, key)

	// if length creater than the index then writing to the client

	if stat{

		// writing to the tcp socket

		_, err := sock.Conn.Write(packetBuffer.Array())
		
		if err != nil {
		
			go ChannelList.WriteLog(err.Error())

			return

		}

	}
}

// checking if directory exists and creating if does not exists, used for offset management

func checkCreateDirectory(conn net.TCPConn, packetObject pojo.PacketStruct, checkDirectoryChan chan bool){

	defer ChannelList.Recover()

	// declaring consumerName

	consumerName := packetObject.ChannelName + packetObject.SubscriberName

	// declaring directory path

	directoryPath := ChannelList.TCPStorage[packetObject.ChannelName].Path+"/"+consumerName

	// getting the os stat if null then return true

	if _, err := os.Stat(directoryPath); err == nil{

		checkDirectoryChan <- true

	}else if os.IsNotExist(err){ // if not exists then create directory

		errDir := os.MkdirAll(directoryPath, 0755)

		if errDir != nil { //  if error then throw error
			
			ThroughClientError(conn, err.Error())

			// deleting the subscriber from the list, locking the channel list array using mutex

			DeleteTCPChannelSubscriberList(packetObject.ChannelName, consumerName)

			checkDirectoryChan <- false

			return

		}

		// else subscriber directory created return true

		go ChannelList.WriteLog("Subscriber directory created successfully...")

		checkDirectoryChan <- true

	}else{

		checkDirectoryChan <- true

	}
}

// create subscriber offset file, used for persistent channels

func createSubscriberOffsetFile(index int, conn net.TCPConn, packetObject pojo.PacketStruct, start_from string, partitionOffsetSubscriber chan int64){

	defer ChannelList.Recover()

	// declaring consumer name

	consumerName := packetObject.ChannelName + packetObject.SubscriberName

	// declaring directory path

	directoryPath := ChannelList.TCPStorage[packetObject.ChannelName].Path+"/"+consumerName

	// declaring the consumer offset path

	consumerOffsetPath := directoryPath+"\\"+packetObject.SubscriberName+"_offset_"+strconv.Itoa(index)+".index"

	// checking the os stat of the path

	if _, err := os.Stat(consumerOffsetPath); err == nil{

		// if the start from flag is beginning then the offset will bet set to 0

		if start_from == "BEGINNING"{

			partitionOffsetSubscriber <- 0

		}else if start_from == "NOPULL"{ // if no pull the offset will be equals to filelength

			partitionOffsetSubscriber <- (-1)

		}else if start_from == "LASTRECEIVED"{ // if last received then it will read the offset file and set the offset

			dat, err := ioutil.ReadFile(consumerOffsetPath)

			if err != nil{ // if error not equals to null then error

				ThroughClientError(conn, err.Error())

				DeleteTCPChannelSubscriberList(packetObject.ChannelName, consumerName)

				partitionOffsetSubscriber <- 0

				return

			} 

			// checking the length of the data that is being read from the file, if error then set to 0 else offet that is in the file

			if len(dat) == 0{

				partitionOffsetSubscriber <- 0

			}else{

				partitionOffsetSubscriber <- int64(binary.BigEndian.Uint64(dat))

			}

		}else{

			partitionOffsetSubscriber <- 0

		}

		// opening the file and setting file descriptor

		fDes, err := os.OpenFile(consumerOffsetPath,
			os.O_WRONLY, os.ModeAppend)

		// if error 

		if err != nil {

			ThroughClientError(conn, err.Error())

			DeleteTCPChannelSubscriberList(packetObject.ChannelName, consumerName)

			partitionOffsetSubscriber <- 0

			return
		}

		// adding file descriptor object to packetObject

		AddSubscriberFD(index, packetObject, fDes)

	}else if os.IsNotExist(err){ // if error in file existence

		// creating file

		fDes, err := os.Create(consumerOffsetPath)

		if err != nil{

			ThroughClientError(conn, err.Error())

			DeleteTCPChannelSubscriberList(packetObject.ChannelName, consumerName)

			partitionOffsetSubscriber <- 0

			return

		}

		// then setting the file descriptor

		AddSubscriberFD(index, packetObject, fDes)

		partitionOffsetSubscriber <- 0

	}else{

		// if start_from is BEGINNING, then offset will be 0

		if start_from == "BEGINNING"{

			partitionOffsetSubscriber <- 0

		}else if start_from == "NOPULL"{ // if NOPULL the -1, means offset will be total file length

			partitionOffsetSubscriber <- (-1)

		}else if start_from == "LASTRECEIVED"{ // if LASTRECEIVED then it will read file and get the last offset received by the file

			dat, err := ioutil.ReadFile(consumerOffsetPath)

			if err != nil{

				ThroughClientError(conn, err.Error())

				DeleteTCPChannelSubscriberList(packetObject.ChannelName, consumerName)

				partitionOffsetSubscriber <- 0

				return

			}

			// length of the data == 0 then offset will be 0

			if len(dat) == 0{

				partitionOffsetSubscriber <- 0

			}else{ // else the data that is in the file

				partitionOffsetSubscriber <- int64(binary.BigEndian.Uint64(dat))

			}

		}else{

			// sending offset as 0

			partitionOffsetSubscriber <- 0

		}

	}

}

// create a directory for subscriber group

func checkCreateGroupDirectory(channelName string, groupName string, checkDirectoryChan chan bool){

	defer ChannelList.Recover()

	// setting directory path

	directoryPath := ChannelList.TCPStorage[channelName].Path+"/"+groupName

	// getting the stat of the directory path

	if _, err := os.Stat(directoryPath); err == nil{

		checkDirectoryChan <- true

	}else if os.IsNotExist(err){ // checking if file exists

		errDir := os.MkdirAll(directoryPath, 0755)

		if errDir != nil { // if not equals to null then error
			
			ThroughGroupError(channelName, groupName, err.Error())

			checkDirectoryChan <- false

			return

		}

		// directory created successfully

		go ChannelList.WriteLog("Subscriber directory created successfully...")

		checkDirectoryChan <- true

	}else{

		checkDirectoryChan <- true

	}

}

// create subscriber group offset

func createSubscriberGroupOffsetFile(index int, channelName string, groupName string, packetObject pojo.PacketStruct, partitionOffsetSubscriber chan int64, start_from string){

	defer ChannelList.Recover()

	// set directory path

	directoryPath := ChannelList.TCPStorage[channelName].Path+"/"+groupName

	// setting consumer offset path

	consumerOffsetPath := directoryPath+"\\"+groupName+"_offset_"+strconv.Itoa(index)+".index"

	// getting the os stat

	if _, err := os.Stat(consumerOffsetPath); err == nil{

		// start_from == BEGINNING then offset = 0 means it will start reading file from beginning

		if start_from == "BEGINNING"{

			partitionOffsetSubscriber <- 0

		}else if start_from == "NOPULL"{ // if start_from == NOPULL then offset = -1 then offset = file size

			partitionOffsetSubscriber <- (-1)

		}else if start_from == "LASTRECEIVED"{ // if start_from == LASTRECEIVED then offset = last offset written into the file

			// reading the file to get the last offset of the subscriber 

			dat, err := ioutil.ReadFile(consumerOffsetPath)

			if err != nil{

				ThroughGroupError(channelName, groupName, err.Error())

				partitionOffsetSubscriber <- 0

				return

			}

			// if data fetched from file === 0 then offset will be 0 else from file

			if len(dat) == 0{

				partitionOffsetSubscriber <- 0

			}else{

				partitionOffsetSubscriber <- int64(binary.BigEndian.Uint64(dat))

			}

		}else{

			// offset = 0

			partitionOffsetSubscriber <- 0

		}

		// getting the file descriptor object and setting to packetObject

		fDes, err := os.OpenFile(consumerOffsetPath,
			os.O_WRONLY, os.ModeAppend) //race

		if err != nil {

			ThroughGroupError(channelName, groupName, err.Error())

			partitionOffsetSubscriber <- 0

			return
		}

		// adding to fD to packetObject

		AddSubscriberFD(index, packetObject, fDes) // race

	}else if os.IsNotExist(err){

		// if not exists then create a offset file

		fDes, err := os.Create(consumerOffsetPath)

		if err != nil{

			ThroughGroupError(channelName, groupName, err.Error())

			partitionOffsetSubscriber <- 0

			return

		}

		// then adding the file descriptor object

		AddSubscriberFD(index, packetObject, fDes)

		partitionOffsetSubscriber <- 0

	}else{

		// start_from == BEGINNING then offset = 0 means it will start reading file from beginning

		if start_from == "BEGINNING"{

			partitionOffsetSubscriber <- 0

		}else if start_from == "NOPULL"{ // if start_from == NOPULL then offset = -1 then offset = file size

			partitionOffsetSubscriber <- (-1)

		}else if start_from == "LASTRECEIVED"{ // if start_from == LASTRECEIVED then offset = last offset written into the file

			// reading the file to get the last offset of the subscriber 

			dat, err := ioutil.ReadFile(consumerOffsetPath)

			if err != nil{

				ThroughGroupError(channelName, groupName, err.Error())

				partitionOffsetSubscriber <- 0

				return

			}	

			// if data fetched from file === 0 then offset will be 0 else from file

			if len(dat) == 0{

				partitionOffsetSubscriber <- 0

			}else{

				// read the file bytes

				partitionOffsetSubscriber <- int64(binary.BigEndian.Uint64(dat))

			}

		}else{

			// offset = 0 reading from beginning of the file

			partitionOffsetSubscriber <- 0

		}

	}

}

/*
	Subscriber Group Channel Method, here in this method the subscriber listens to file change in seperate go routines and publishes to subscriber
*/

func SubscribeGroupChannel(channelName string, groupName string, packetObject pojo.PacketStruct, start_from string){

	defer ChannelList.Recover()

	// creating channels for creating directory

	checkDirectoryChan := make(chan bool, 1)
	defer close(checkDirectoryChan)


	// offsetByteSize

	offsetByteSize := make([]int64, ChannelList.TCPStorage[packetObject.ChannelName].PartitionCount)

	// creating channels for partition offsets

	partitionOffsetSubscriber := make(chan int64, 1)
	defer close(partitionOffsetSubscriber)

	// checking for directory existence

	go checkCreateGroupDirectory(channelName, groupName, checkDirectoryChan)

	if false == <-checkDirectoryChan{

		return

	}

	// setting file descriptor 

	packetObject.SubscriberFD = CreateSubscriberGrpFD(packetObject.ChannelName)

	for i:=0;i<ChannelList.TCPStorage[packetObject.ChannelName].PartitionCount;i++{

		go createSubscriberGroupOffsetFile(i, channelName, groupName, packetObject, partitionOffsetSubscriber, start_from) // race

		offsetByteSize[i] = <-partitionOffsetSubscriber

	}

	// checking file descriptor length for all partitions

	if len(packetObject.SubscriberFD) == 0{

		ThroughGroupError(channelName, groupName, INVALID_SUBSCRIBER_OFFSET)

		return

	}

	// iterating to all partitions and start listening to file change with go routines

	for i:=0;i<ChannelList.TCPStorage[packetObject.ChannelName].PartitionCount;i++{

		go func(index int, cursor int64, packetObject pojo.PacketStruct){

			defer ChannelList.Recover()

			// declaring a mutex variable

			var groupMtx sync.Mutex

			// setting the file path to read the log file

			filePath := ChannelList.TCPStorage[packetObject.ChannelName].Path+"/"+packetObject.ChannelName+"_partition_"+strconv.Itoa(index)+".br"

			// opening the file

			file, err := os.Open(filePath)

			if err != nil {

				ThroughGroupError(packetObject.ChannelName, packetObject.GroupName, err.Error())

				return
			}

			defer file.Close()

			// creating a sent message boolean channel

			sentMsg := make(chan bool, 1)
			defer close(sentMsg)

			// setting a exitLoop variable which will stop the infinite loop when the subscriber is disconnected

			exitLoop := false

			for{

				// if exitLoop == true then break and close file desciptor

				if exitLoop{

					consumerGroupLen := GetChannelGrpMapLen(packetObject.ChannelName, packetObject.GroupName)

					if consumerGroupLen <= 0{

						go CloseSubscriberGrpFD(packetObject)

					}

					break

				}

				// getting the file stat

				fileStat, err := os.Stat(filePath)
		 
				if err != nil {

					ThroughGroupError(packetObject.ChannelName, packetObject.GroupName, err.Error())

					exitLoop = true
					
					continue
					
				}

				// if cursor == -1 then cursor  = file size

				if cursor == -1{

					cursor = fileStat.Size()

				}

				// if cursor  >= file size then skip the iteration

				if cursor >= fileStat.Size(){

					time.Sleep(1 * time.Second)

					continue

				}

				// creating a data byte array of long data type


				data := make([]byte, 8)

				// reading the file at the exact cursor count

				count, err := file.ReadAt(data, cursor)

				if err != nil {

					time.Sleep(1 * time.Second)

					continue

				}

				// converting the packet to big endian int64

				packetSize := binary.BigEndian.Uint64(data)

				// if packet size is greater then file size then skip the iteration

				if int64(count) <= 0 || int64(packetSize) >= fileStat.Size(){

					time.Sleep(1 * time.Second)

					continue

				}

				// adding 8 number to cursor

				cursor += int64(8)

				// creating byte array of packet size

				restPacket := make([]byte, int64(packetSize))

				// reading from file at the cursor count

				totalByteLen, errPacket := file.ReadAt(restPacket, cursor)

				if errPacket != nil{

					time.Sleep(1 * time.Second)

					continue

				}

				// if totalByteLen <= 0 then skip the iteration

				if totalByteLen <= 0{

					time.Sleep(1 * time.Second)

					continue
				}

				// adding the packet size to cursor

				cursor += int64(packetSize)

				// creating a byte buffer of type big endian

				byteFileBuffer := ByteBuffer.Buffer{
					Endian:"big",
				}
				// wrapping the restPacket that is fetched from the file

				byteFileBuffer.Wrap(restPacket)

				// setting them to local variables

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

				// creating another byte buffer in big endian 

				byteSendBuffer := ByteBuffer.Buffer{
					Endian:"big",
				}

				// adding values to the byte buffer along with packet header

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

				// sending to group and waiting for call back

				go sendGroup(index, groupMtx, int(cursor), packetObject, byteSendBuffer, sentMsg) // race

				// waiting for callback

				message, ok := <-sentMsg

				if ok{

					// if message if false then loop will exit

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

/*
	method to publish messages to individual subscriber in case of persistent channel
*/

func SubscribeChannel(conn net.TCPConn, packetObject pojo.PacketStruct, start_from string){

	defer ChannelList.Recover()

	//setting consumer name

	consumerName := packetObject.ChannelName + packetObject.SubscriberName

	// creating directory channel boolean

	checkDirectoryChan := make(chan bool, 1)
	defer close(checkDirectoryChan)

	// creating offset byte size variable

	offsetByteSize := make([]int64, ChannelList.TCPStorage[packetObject.ChannelName].PartitionCount)

	// creating boolean channel for offset subscriber

	partitionOffsetSubscriber := make(chan int64, 1)
	defer close(partitionOffsetSubscriber)

	// creating offset directory

	go checkCreateDirectory(conn, packetObject, checkDirectoryChan)

	// waiting for its callback

	if false == <-checkDirectoryChan{

		return

	}

	// creating file descriptor

	packetObject.SubscriberFD = make([]*os.File, ChannelList.TCPStorage[packetObject.ChannelName].PartitionCount)

	// iterating over the partition count and adding file desciptor object

	for i:=0;i<ChannelList.TCPStorage[packetObject.ChannelName].PartitionCount;i++{

		go createSubscriberOffsetFile(i, conn, packetObject, start_from, partitionOffsetSubscriber)

		offsetByteSize[i] = <-partitionOffsetSubscriber

	}

	// checking file descriptor length

	if len(packetObject.SubscriberFD) == 0{

		ThroughClientError(conn, INVALID_SUBSCRIBER_OFFSET)

		DeleteTCPChannelSubscriberList(packetObject.ChannelName, consumerName)

		return

	}

	// creating a subscriberMtx mutex

	var subscriberMtx sync.Mutex

	// iterating over the paritition count to start listening to file change using go routines

	for i:=0;i<ChannelList.TCPStorage[packetObject.ChannelName].PartitionCount;i++{

		go func(index int, cursor int64, conn net.TCPConn, packetObject pojo.PacketStruct){

			defer ChannelList.Recover()

			// setting file path of the logs

			filePath := ChannelList.TCPStorage[packetObject.ChannelName].Path+"/"+packetObject.ChannelName+"_partition_"+strconv.Itoa(index)+".br"

			// opening file 

			file, err := os.Open(filePath)

			defer file.Close()

			if err != nil {
				
				ThroughClientError(conn, err.Error())

				DeleteTCPChannelSubscriberList(packetObject.ChannelName, consumerName)

				return
			}

			// creating sent message boolean channel

			sentMsg := make(chan bool, 1)
			defer close(sentMsg)

			// creating exitLoop variable

			exitLoop := false

			for{

				// if exitLoop == true the break from iteration, it will be true when subscriber disconnects

				if exitLoop{

					conn.Close()

					go CloseSubscriberGrpFD(packetObject)

					DeleteTCPChannelSubscriberList(packetObject.ChannelName, consumerName)

					break

				}

				// reading the file stat

				fileStat, err := os.Stat(filePath)
		 
				if err != nil {

					ThroughClientError(conn, err.Error())

					DeleteTCPChannelSubscriberList(packetObject.ChannelName, consumerName)

					break
					
				}

				// cursor == -1 then cursor == file size

				if cursor == -1{

					cursor = fileStat.Size()

				}

				// if cursor >=  file size then skipping the iteration

				if cursor >= fileStat.Size(){

					time.Sleep(1 * time.Second)

					continue

				}

				// creating 8 byte empty array

				data := make([]byte, 8)

				// reading the file at the cursor point

				count, err := file.ReadAt(data, cursor)

				if err != nil {

					time.Sleep(1 * time.Second)

					continue

				}

				// converting the packet size to big endian

				packetSize := binary.BigEndian.Uint64(data)

				if int64(count) <= 0 || int64(packetSize) >= fileStat.Size(){

					time.Sleep(1 * time.Second)

					continue

				}

				// adding 8 number to cursor

				cursor += int64(8)

				// creating restPacket byte array of packetSize

				restPacket := make([]byte, packetSize)

				// reading the file to exact cursor counter

				totalByteLen, errPacket := file.ReadAt(restPacket, cursor)

				if errPacket != nil{

					time.Sleep(1 * time.Second)

					continue

				}

				// if total byte length == 0 skipping the iteration

				if totalByteLen <= 0{

					time.Sleep(1 * time.Second)

					continue

				}

				// adding packetsize to cursor

				cursor += int64(packetSize)

				// creating byte buffer in big endian

				byteFileBuffer := ByteBuffer.Buffer{
					Endian:"big",
				}

				// wrapping the restPacket

				byteFileBuffer.Wrap(restPacket)

				// setting the values to local variables

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

				// creating bytebuffer of big endian and setting the values to be published

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

				// sending to the subscriber and waiting for callback

				go send(index, int(cursor), subscriberMtx, packetObject, conn, byteSendBuffer, sentMsg)

				// waiting for callbacks

				message, ok := <-sentMsg

				if ok{

					// if message == false then exitLoop == true and loop will be breaked

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

/*
	method to send data to subscriber group
*/

func sendGroup(index int, groupMtx sync.Mutex, cursor int, packetObject pojo.PacketStruct, packetBuffer ByteBuffer.Buffer, sentMsg chan bool){

	defer ChannelList.Recover()

	// locking the method using mutex to prevent concurrent write to tcp

	groupMtx.Lock()

	defer groupMtx.Unlock()

	groupId := 0

	var group *pojo.PacketStruct

	// setting a state of retry if disconnection happens then the data should be written to another subscriber

	RETRY:

	group, groupId = GetValue(packetObject.ChannelName, packetObject.GroupName, &groupId, index)

	if group == nil{

		sentMsg <- false

		return
	}

	// writing to tcp 
	
	_, err := group.Conn.Write(packetBuffer.Array())
	
	if err != nil {

		group, groupId = GetValue(packetObject.ChannelName, packetObject.GroupName, &groupId, index)

		if group == nil{

			sentMsg <- false

			return
		}

		goto RETRY

	}

	// creating subscriber offset and writing into subscriber offset file

	byteArrayCursor := make([]byte, 8)
	binary.BigEndian.PutUint64(byteArrayCursor, uint64(cursor))

	sentMsg <- WriteSubscriberGrpOffset(index, packetObject, byteArrayCursor)
}

/*
	method to send message to subscriber
*/

func send(index int, cursor int, subscriberMtx sync.Mutex, packetObject pojo.PacketStruct, conn net.TCPConn, packetBuffer ByteBuffer.Buffer, sentMsg chan bool){ 

	defer ChannelList.Recover()

	// locking the method using mutex to prevent concurrent write to tcp

	subscriberMtx.Lock()
	defer subscriberMtx.Unlock()

	_, err := conn.Write(packetBuffer.Array())
	
	if err != nil {
	
		go ChannelList.WriteLog(err.Error())

		sentMsg <- false

		return
	}

	// creating subscriber offset and writing into subscriber offset file

	byteArrayCursor := make([]byte, 8)
	binary.BigEndian.PutUint64(byteArrayCursor, uint64(cursor))

	sentMsg <- WriteSubscriberGrpOffset(index, packetObject, byteArrayCursor)

}

func (e *ChannelMethods) SendAck(messageMap pojo.PacketStruct, ackChan chan bool){

	defer ChannelList.Recover()

	// creating byte buffer to send acknowledgement to producer

	byteBuffer := ByteBuffer.Buffer{
		Endian:"big",
	}

	byteBuffer.PutLong(len(messageMap.Producer_id))

	byteBuffer.PutByte(byte(2)) // status code

	byteBuffer.Put([]byte(messageMap.Producer_id))

	// writing to tcp socket

	_, err := messageMap.Conn.Write(byteBuffer.Array())

	if err != nil{

		go ChannelList.WriteLog(err.Error())

		ackChan <- false

		return
	}

	ackChan <- true

}

// to be member for the future development contact @sudeep@pounze.com