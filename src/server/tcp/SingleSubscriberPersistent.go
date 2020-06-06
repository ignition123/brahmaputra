package tcp

import(
	"net"
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

/*
	method to publish messages to individual subscriber in case of persistent channel
*/

func SubscribeChannel(conn net.TCPConn, packetObject pojo.PacketStruct, start_from string, socketDisconnect *bool){

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

				if exitLoop || *socketDisconnect{

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

				CompressionType := byteFileBuffer.GetByte()

				bodyPacketSize := int64(packetSize) - int64(2 + messageTypeLen + 2 + channelNameLen + 2 + producer_idLen + 2 + agentNameLen + 8 + 1)

				bodyBB := byteFileBuffer.Get(int(bodyPacketSize))

				newTotalByteLen := 2 + messageTypeLen + 2 + channelNameLen + 2 + producer_idLen + 2 + agentNameLen + 8 + 1 + len(bodyBB)

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

				byteSendBuffer.PutByte(CompressionType[0]) // compression type

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