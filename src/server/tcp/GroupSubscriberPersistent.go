package tcp

import(
	"pojo"
	"ChannelList"
	"os"
	"strconv"
	"io/ioutil"
	"sync"
	"time"
	"encoding/binary"
	"ByteBuffer"
)

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

func SubscribeGroupChannel(channelName string, groupName string, packetObject pojo.PacketStruct, start_from string, socketDisconnect *bool){

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

				// socket client disconnected checking the length of the group if 0 then closing the loop and file descriptor

				if *socketDisconnect{

					consumerGroupLen := GetChannelGrpMapLen(packetObject.ChannelName, packetObject.GroupName)

					if consumerGroupLen <= 0{

						go CloseSubscriberGrpFD(packetObject)

						break
					}

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

				CompressionType := byteFileBuffer.GetByte()

				bodyPacketSize := int64(packetSize) - int64(2 + messageTypeLen + 2 + channelNameLen + 2 + producer_idLen + 2 + agentNameLen + 8 + 1)

				bodyBB := byteFileBuffer.Get(int(bodyPacketSize))

				newTotalByteLen := 2 + messageTypeLen + 2 + channelNameLen + 2 + producer_idLen + 2 + agentNameLen + 8 + 1 + len(bodyBB)

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

				byteSendBuffer.PutByte(CompressionType[0]) // compression type

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