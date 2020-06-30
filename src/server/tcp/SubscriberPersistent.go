package tcp

import(
	"ChannelList"
	"pojo"
	"os"
	"strconv"
	"io/ioutil"
	"encoding/binary"
	_"sync"
	"time"
	"ByteBuffer"
	_"log"
)

func checkCreateDirectory(clientObj *pojo.ClientObject, packetObject *pojo.PacketStruct, checkDirectoryChan chan bool){

	defer ChannelList.Recover()

	// declaring consumerName

	consumerName := packetObject.ChannelName + packetObject.SubscriberName

	// declaring directory path

	directoryPath := pojo.SubscriberObj[packetObject.ChannelName].Channel.Path+"/"+consumerName

	if _, err := os.Stat(directoryPath); err == nil{

		checkDirectoryChan <- true

	}else if os.IsNotExist(err){ // if not exists then create directory

		errDir := os.MkdirAll(directoryPath, 0755)

		if errDir != nil { //  if error then throw error
			
			ChannelList.ThroughClientError(clientObj.Conn, err.Error())

			// deleting the subscriber from the list, locking the channel list array using mutex

			pojo.SubscriberObj[packetObject.ChannelName].UnRegister <- clientObj

			checkDirectoryChan <- false

			return

		}

		// else subscriber directory created return true

		go ChannelList.WriteLog("Subscriber directory created successfully...")

		checkDirectoryChan <- true

	}else{

		checkDirectoryChan <- false

	}

}

func createSubscriberOffsetFile(index int, clientObj *pojo.ClientObject, packetObject *pojo.PacketStruct, start_from string, partitionOffsetSubscriber chan int64){

	defer ChannelList.Recover()

	// declaring consumer name

	consumerName := packetObject.ChannelName + packetObject.SubscriberName

	// declaring directory path

	directoryPath := pojo.SubscriberObj[packetObject.ChannelName].Channel.Path+"/"+consumerName

	// declaring the consumer offset path

	consumerOffsetPath := directoryPath+"\\"+packetObject.SubscriberName+"_offset_"+strconv.Itoa(index)+".index"

	// checking the os stat of the path

	if _, err := os.Stat(consumerOffsetPath); err == nil{

		// opening the file and setting file descriptor

		fDes, err := os.OpenFile(consumerOffsetPath,
			os.O_WRONLY, os.ModeAppend)

		// if error 

		if err != nil {

			ChannelList.ThroughClientError(clientObj.Conn, err.Error())

			pojo.SubscriberObj[packetObject.ChannelName].UnRegister <- clientObj

			partitionOffsetSubscriber <- int64(-2)

			return
		}

		// adding file descriptor object to packetObject

		packetObject.SubscriberFD[index] = fDes

		// if the start from flag is beginning then the offset will bet set to 0

		if start_from == "BEGINNING"{

			partitionOffsetSubscriber <- int64(0)

		}else if start_from == "NOPULL"{ // if no pull the offset will be equals to filelength

			partitionOffsetSubscriber <- int64(-1)

		}else if start_from == "LASTRECEIVED"{ // if last received then it will read the offset file and set the offset

			dat, err := ioutil.ReadFile(consumerOffsetPath)

			if err != nil{ // if error not equals to null then error

				ChannelList.ThroughClientError(clientObj.Conn, err.Error())

				pojo.SubscriberObj[packetObject.ChannelName].UnRegister <- clientObj

				partitionOffsetSubscriber <- int64(-2)

				return

			} 

			// checking the length of the data that is being read from the file, if error then set to 0 else offet that is in the file

			if len(dat) == 0{

				partitionOffsetSubscriber <- int64(0)

			}else{

				partitionOffsetSubscriber <- int64(binary.BigEndian.Uint64(dat))

			}

		}else{

			partitionOffsetSubscriber <- int64(0)

		}

	}else if os.IsNotExist(err){ // if error in file existence

		// creating file

		fDes, err := os.Create(consumerOffsetPath)

		if err != nil{

			ChannelList.ThroughClientError(clientObj.Conn, err.Error())

			pojo.SubscriberObj[packetObject.ChannelName].UnRegister <- clientObj

			partitionOffsetSubscriber <- int64(-2)

			return

		}

		// then setting the file descriptor

		packetObject.SubscriberFD[index] = fDes

		if start_from == "BEGINNING"{

			partitionOffsetSubscriber <- int64(0)

		}else if start_from == "NOPULL"{

			partitionOffsetSubscriber <- int64(-1)

		}

	}else{

		// if start_from is BEGINNING, then offset will be 0

		if start_from == "BEGINNING"{

			partitionOffsetSubscriber <- int64(0)

		}else if start_from == "NOPULL"{ // if NOPULL the -1, means offset will be total file length

			partitionOffsetSubscriber <- int64(-1)

		}else if start_from == "LASTRECEIVED"{ // if LASTRECEIVED then it will read file and get the last offset received by the file

			dat, err := ioutil.ReadFile(consumerOffsetPath)

			if err != nil{

				ChannelList.ThroughClientError(clientObj.Conn, err.Error())

				pojo.SubscriberObj[packetObject.ChannelName].UnRegister <- clientObj

				partitionOffsetSubscriber <- int64(-2)

				return

			}

			// length of the data == 0 then offset will be 0

			if len(dat) == 0{

				partitionOffsetSubscriber <- int64(0)

			}else{ // else the data that is in the file

				partitionOffsetSubscriber <- int64(binary.BigEndian.Uint64(dat))

			}

		}else{

			// sending offset as 0

			partitionOffsetSubscriber <- int64(0)

		}

	}

}

func SubscriberSinglePersistent(clientObj *pojo.ClientObject,  packetObject *pojo.PacketStruct, start_from string){

	defer ChannelList.Recover()

	// creating offset byte size variable

	offsetByteSize := make([]int64, pojo.SubscriberObj[packetObject.ChannelName].Channel.PartitionCount)

	// creating directory channel boolean

	checkDirectoryChan := make(chan bool, 1)
	defer close(checkDirectoryChan)

	// creating boolean channel for offset subscriber

	partitionOffsetSubscriber := make(chan int64, 1)
	defer close(partitionOffsetSubscriber)

	// creating offset directory

	go checkCreateDirectory(clientObj, packetObject, checkDirectoryChan)

	// waiting for its callback

	if false == <-checkDirectoryChan{

		pojo.SubscriberObj[packetObject.ChannelName].UnRegister <- clientObj

		return

	}

	// creating file descriptor

	packetObject.SubscriberFD = make([]*os.File, pojo.SubscriberObj[packetObject.ChannelName].Channel.PartitionCount)

	// iterating over the partition count and adding file desciptor object

	filesOpenedFailed := false

	for i:=0;i<pojo.SubscriberObj[packetObject.ChannelName].Channel.PartitionCount;i++{

		go createSubscriberOffsetFile(i, clientObj, packetObject, start_from, partitionOffsetSubscriber)

		offsetByteSize[i] =  <-partitionOffsetSubscriber

		if offsetByteSize[i] == -2{

			filesOpenedFailed = true
		}

	}

	if filesOpenedFailed{

		ChannelList.ThroughClientError(clientObj.Conn, ChannelList.INVALID_SUBSCRIBER_OFFSET)

		pojo.SubscriberObj[packetObject.ChannelName].UnRegister <- clientObj

		return
	}

	// checking file descriptor length

	if len(packetObject.SubscriberFD) == 0{

		ChannelList.ThroughClientError(clientObj.Conn, ChannelList.INVALID_SUBSCRIBER_OFFSET)

		pojo.SubscriberObj[packetObject.ChannelName].UnRegister <- clientObj

		return

	}

	closeChannel := make(chan bool, 1)

	// running subscriber listener

	go packetSinglePersistentListener(clientObj, closeChannel, packetObject)

	// iterating over the paritition count to start listening to file change using go routines

	for i:=0;i<pojo.SubscriberObj[packetObject.ChannelName].Channel.PartitionCount;i++{

		go func(index int, cursor int64, clientObj *pojo.ClientObject, packetObject *pojo.PacketStruct){

			defer ChannelList.Recover()

			// setting file path of the logs

			filePath := pojo.SubscriberObj[packetObject.ChannelName].Channel.Path+"/"+packetObject.ChannelName+"_partition_"+strconv.Itoa(index)+".br"

			// opening file 

			file, err := os.Open(filePath)

			defer file.Close()

			if err != nil {
				
				ChannelList.ThroughClientError(clientObj.Conn, err.Error())

				pojo.SubscriberObj[packetObject.ChannelName].UnRegister <- clientObj

				return
			}

			// creating parentExitLoop variable

			parentExitLoop:
				for{

					// reading the file stat

					fileStat, err := os.Stat(filePath)
			 
					if err != nil {

						ChannelList.ThroughClientError(clientObj.Conn, err.Error())

						select{
							case pojo.SubscriberObj[packetObject.ChannelName].UnRegister <- clientObj:
							case clientObj.Channel <- nil:
								break parentExitLoop
						}

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

					select{

						case clientObj.Channel <- &pojo.PublishMsg{
							Index: index,
							Cursor: cursor,
							Msg: byteSendBuffer.Array(),
						}:
						break

						case _, channStat := <- closeChannel:

							if channStat{

								break parentExitLoop

							}
						break

					}
 
				}

			packetObject.SubscriberFD[index].Close()

			go ChannelList.WriteLog("Socket group subscribers file reader closed...")

		}(i, offsetByteSize[i], clientObj, packetObject)

	}
	
}

func packetSinglePersistentListener(clientObj *pojo.ClientObject, closeChannel chan bool, packetObject *pojo.PacketStruct){

	defer ChannelList.Recover()

	exitLoop:

		for msg := range clientObj.Channel{

			if msg == nil || !sendMessageToClientPersistent(clientObj, msg, packetObject){

				break exitLoop

			}

		}

	clientObj.Conn.Close()

	go ChannelList.WriteLog("Socket group subscriber channel closed...")

	closeChannel <- true

}

func sendMessageToClientPersistent(clientObj *pojo.ClientObject, message *pojo.PublishMsg, packetObject *pojo.PacketStruct) bool{ 

	defer ChannelList.Recover()

	_, err := clientObj.Conn.Write(message.Msg)
	
	if err != nil {
	
		go ChannelList.WriteLog(err.Error())

		return false
	}

	// creating subscriber offset and writing into subscriber offset file

	byteArrayCursor := make([]byte, 8)
	binary.BigEndian.PutUint64(byteArrayCursor, uint64(message.Cursor))

	return ChannelList.WriteSubscriberOffset(message.Index, packetObject, clientObj, byteArrayCursor)

}