package tcp

import(
	"time"
	"ByteBuffer"
	"objects"
	"ChannelList"
	"os"
	"encoding/binary"
	"strconv"
)

func SubscribeGroupChannel(channelName string, groupName string, packetObject *objects.PacketStruct, clientObj *objects.ClientObject, start_from string){

	defer ChannelList.Recover()

	// offsetByteSize

	offsetByteSize := make([]int64, objects.SubscriberObj[packetObject.ChannelName].Channel.PartitionCount)

	// creating channels for creating directory

	checkDirectoryChan := make(chan bool, 1)
	defer close(checkDirectoryChan)

	// creating channels for partition offsets

	partitionOffsetSubscriber := make(chan int64, 1)
	defer close(partitionOffsetSubscriber)

	// checking for directory existence

	go checkCreateGroupDirectory(channelName, groupName, clientObj, checkDirectoryChan)

	if false == <-checkDirectoryChan{

		objects.SubscriberObj[channelName].GroupUnRegister <- groupName

		return

	}

	// setting file descriptor 

	packetObject.SubscriberFD = ChannelList.CreateSubscriberGrpFD(packetObject.ChannelName)

	filesOpenedFailed := false

	for i:=0;i<objects.SubscriberObj[packetObject.ChannelName].Channel.PartitionCount;i++{

		go createSubscriberGroupOffsetFile(i, channelName, groupName, packetObject, start_from, clientObj, partitionOffsetSubscriber) // race

		offsetByteSize[i] = <- partitionOffsetSubscriber

		if offsetByteSize[i] == -2{

			filesOpenedFailed = true
		}

	}

	if filesOpenedFailed{

		ChannelList.ThroughGroupError(channelName, groupName, ChannelList.INVALID_SUBSCRIBER_OFFSET)

		objects.SubscriberObj[channelName].GroupUnRegister <- groupName

		return
	}

	// checking file descriptor length for all partitions

	if len(packetObject.SubscriberFD) == 0{

		ChannelList.ThroughGroupError(channelName, groupName, ChannelList.INVALID_SUBSCRIBER_OFFSET)

		objects.SubscriberObj[channelName].GroupUnRegister <- groupName

		return

	}

	// declaring a mutex variable

	groupMsgChan := make(chan *objects.PublishMsg)

	closeChannel := make(chan bool, 1)

	go packetGroupPersistentListener(channelName, groupName, packetObject, groupMsgChan, closeChannel)

	// iterating to all partitions and start listening to file change with go routines

	for i:=0;i<objects.SubscriberObj[packetObject.ChannelName].Channel.PartitionCount;i++{

		go func(index int, cursor int64, packetObject *objects.PacketStruct, clientObj *objects.ClientObject, groupMsgChan chan *objects.PublishMsg){

			defer ChannelList.Recover()

			// setting the file path to read the log file

			filePath := objects.SubscriberObj[packetObject.ChannelName].Channel.Path+"/"+packetObject.ChannelName+"_partition_"+strconv.Itoa(index)+".br"

			// opening the file

			file, err := os.Open(filePath)

			if err != nil {

				ChannelList.ThroughGroupError(packetObject.ChannelName, packetObject.GroupName, err.Error())

				objects.SubscriberObj[channelName].GroupUnRegister <- groupName

				return
			}

			defer file.Close()

			// setting a exitLoop variable which will stop the infinite loop when the subscriber is disconnected

			exitParentLoop:

				for{
					// getting the file stat

					fileStat, err := os.Stat(filePath)
			 
					if err != nil {

						ChannelList.ThroughGroupError(packetObject.ChannelName, packetObject.GroupName, err.Error())

						select{

							case objects.SubscriberObj[packetObject.ChannelName].UnRegister <- clientObj:
							case groupMsgChan <- nil:
								break exitParentLoop
						}

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

					// sending to group and waiting for call back

					select{
						
						case groupMsgChan <- &objects.PublishMsg{
							Index: index,
							Cursor: cursor,
							Msg: byteSendBuffer.Array(),
						}:
						break

						case _, channStat := <-closeChannel:

							if channStat{

								break exitParentLoop
							}

						break

						case <-time.After(5 * time.Second):
						break

					}

				}

			packetObject.SubscriberFD[index].Close()

			go ChannelList.WriteLog("Socket group subscribers file reader closed...")

		}(i, offsetByteSize[i], packetObject, clientObj, groupMsgChan)

	}
}

func packetGroupPersistentListener(channelName string, groupName string, packetObject *objects.PacketStruct, groupMsgChan chan *objects.PublishMsg, closeChannel chan bool){

	defer ChannelList.Recover()

	exitLoop:
		for message := range groupMsgChan{

			if message == nil{

				break exitLoop
			}

			RETRY:

			clientObj, _, groupLen := ChannelList.GetClientObject(channelName, groupName, message.Index)

			if groupLen == 0{

				break exitLoop
			}

			_, err := clientObj.Conn.Write(message.Msg)

			if err != nil {

				goto RETRY

			}

			// creating subscriber offset and writing into subscriber offset file

			byteArrayCursor := make([]byte, 8)
			
			binary.BigEndian.PutUint64(byteArrayCursor, uint64(message.Cursor))

			ChannelList.WriteSubscriberGrpOffset(message.Index, packetObject, byteArrayCursor)
		}

	go ChannelList.WriteLog("Socket group subscriber channel closed...")

	closeChannel <- true

}