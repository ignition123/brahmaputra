package tcp

import(
	"time"
	"ByteBuffer"
	"objects"
	"ChannelList"
	"os"
	"encoding/binary"
	"sync/atomic"
	"strconv"
)

func SubscriberSinglePersistent(clientObj *objects.ClientObject,  packetObject *objects.PacketStruct, start_from string){

	defer ChannelList.Recover()

	// creating offset byte size variable

	offsetByteSize := make([]int64, objects.SubscriberObj[packetObject.ChannelName].Channel.PartitionCount)

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

		objects.SubscriberObj[packetObject.ChannelName].UnRegister <- clientObj

		return

	}

	// creating file descriptor

	packetObject.SubscriberFD = make([]*os.File, objects.SubscriberObj[packetObject.ChannelName].Channel.PartitionCount)

	// iterating over the partition count and adding file desciptor object

	filesOpenedFailed := false

	for i:=0;i<objects.SubscriberObj[packetObject.ChannelName].Channel.PartitionCount;i++{

		go createSubscriberOffsetFile(i, clientObj, packetObject, start_from, partitionOffsetSubscriber)

		offsetByteSize[i] = <-partitionOffsetSubscriber

		if offsetByteSize[i] == -2{

			filesOpenedFailed = true
		}

	}

	if filesOpenedFailed{

		ChannelList.ThroughClientError(clientObj.Conn, ChannelList.INVALID_SUBSCRIBER_OFFSET)

		objects.SubscriberObj[packetObject.ChannelName].UnRegister <- clientObj

		return
	}

	// checking file descriptor length

	if len(packetObject.SubscriberFD) == 0{

		ChannelList.ThroughClientError(clientObj.Conn, ChannelList.INVALID_SUBSCRIBER_OFFSET)

		objects.SubscriberObj[packetObject.ChannelName].UnRegister <- clientObj

		return

	}

	closeChannel := make(chan bool, 1)

	// polling subscriber count

	var pollingCount uint32

	// running subscriber listener

	go packetSinglePersistentListener(clientObj, closeChannel, packetObject, &pollingCount)

	// iterating over the paritition count to start listening to file change using go routines

	for i:=0;i<objects.SubscriberObj[packetObject.ChannelName].Channel.PartitionCount;i++{

		go func(index int, cursor int64, clientObj *objects.ClientObject, packetObject *objects.PacketStruct, pollingCount *uint32){

			defer ChannelList.Recover()

			// setting file path of the logs

			filePath := objects.SubscriberObj[packetObject.ChannelName].Channel.Path+"/"+packetObject.ChannelName+"_partition_"+strconv.Itoa(index)+".br"

			// opening file 

			file, err := os.Open(filePath)

			defer file.Close()

			if err != nil {
				
				ChannelList.ThroughClientError(clientObj.Conn, err.Error())

				objects.SubscriberObj[packetObject.ChannelName].UnRegister <- clientObj

				return
			}

			// creating parentExitLoop variable

			parentExitLoop:

				for{

					// if the client object is true then break the loop

					if clientObj.Disconnection{

						objects.SubscriberObj[packetObject.ChannelName].UnRegister <- clientObj

						clientObj.Channel <- nil

						break parentExitLoop
					}

					// checking if the subscriber type is polling or pushing method

					if clientObj.Polling > 0{

						if !clientObj.StartPoll{

							time.Sleep(10 * time.Millisecond)

							continue
						}

						// commit the last sent offset

						if clientObj.Commit{

							clientObj.Commit = false

							// creating subscriber offset and writing into subscriber offset file

							byteArrayCursor := make([]byte, 8)

							binary.BigEndian.PutUint64(byteArrayCursor, uint64(cursor))

							ChannelList.WriteSubscriberOffset(index, packetObject, clientObj, byteArrayCursor)
						}

					}

					// reading the file stat

					fileStat, err := os.Stat(filePath)
			 
					if err != nil {

						ChannelList.ThroughClientError(clientObj.Conn, err.Error())

						objects.SubscriberObj[packetObject.ChannelName].UnRegister <- clientObj

						clientObj.Channel <- nil

						break parentExitLoop

					}


					// cursor == -1 then cursor == file size

					if cursor == -1{

						cursor = fileStat.Size()

					}

					// if cursor >=  file size then skipping the iteration

					if cursor >= fileStat.Size(){

						if clientObj.Polling > 0{

							SendFinPacket(clientObj)

						}

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

						case clientObj.Channel <- &objects.PublishMsg{
							Index: index,
							Cursor: cursor,
							Msg: byteSendBuffer.Array(),
						}:

						case _, channStat := <- closeChannel:

							if channStat{

								break parentExitLoop

							}

						case <-time.After(5 * time.Second):
					}
 
				}

			packetObject.SubscriberFD[index].Close()

			go ChannelList.WriteLog("Socket group subscribers file reader closed...")

		}(i, offsetByteSize[i], clientObj, packetObject, &pollingCount)

	}
	
}

func packetSinglePersistentListener(clientObj *objects.ClientObject, closeChannel chan bool, packetObject *objects.PacketStruct, pollingCount *uint32){

	defer ChannelList.Recover()

	// send message to client, fetching from channel

	exitLoop:

		for msg := range clientObj.Channel{

			if msg == nil{

				break exitLoop

			}

			sendMessageToClientPersistent(clientObj, msg, packetObject, pollingCount)
		}

	clientObj.Conn.Close()

	go ChannelList.WriteLog("Socket subscriber channel closed...")

	closeChannel <- true

}

func sendMessageToClientPersistent(clientObj *objects.ClientObject, message *objects.PublishMsg, packetObject *objects.PacketStruct, pollingCount *uint32){ 

	defer ChannelList.Recover()

	_, err := clientObj.Conn.Write(message.Msg)
	
	if err != nil {
	
		go ChannelList.WriteLog(err.Error())

		return
	}

	if clientObj.Polling > 0{

		// incrementing the polling the count 

		atomic.AddUint32(pollingCount, 1)

		if int(*pollingCount) >= clientObj.Polling{

			atomic.StoreUint32(pollingCount, 0)

			clientObj.StartPoll = false

			SendFinPacket(clientObj)
		}

	}else{

		// creating subscriber offset and writing into subscriber offset file

		byteArrayCursor := make([]byte, 8)

		binary.BigEndian.PutUint64(byteArrayCursor, uint64(message.Cursor))

		ChannelList.WriteSubscriberOffset(message.Index, packetObject, clientObj, byteArrayCursor)
	}

}