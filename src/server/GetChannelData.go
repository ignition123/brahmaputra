package server

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
)

type ChannelMethods struct{
	sync.Mutex
}

var inmemoryMutex sync.Mutex

func (e *ChannelMethods) GetChannelData(){

	defer ChannelList.Recover()

	for channelName := range ChannelList.TCPStorage {

	    e.runChannel(channelName)

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

								if ChannelList.TCPStorage[channelName].ChannelStorageType == "persistent"{

									go e.SendAck(*message, cbChan)

									<-cbChan

								}else{

									go e.sendMessageToClient(*message, cbChan)

									<-cbChan

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

func SubscribeChannel(conn net.TCPConn, channelName string, cursor int64, quitChannel bool){

	defer ChannelList.Recover()

	file, err := os.Open(ChannelList.TCPStorage[channelName].Path)

	defer file.Close()

	if err != nil {
		go ChannelList.WriteLog(err.Error())
		return
	}

	var sentMsg = make(chan bool, 1)

	defer close(sentMsg)

	var exitLoop = false

	for{

		if exitLoop || quitChannel{

			break
		}

		fileStat, err := os.Stat(ChannelList.TCPStorage[channelName].Path)
 
		if err != nil {
			go ChannelList.WriteLog(err.Error())
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

					var newTotalByteLen = 2 + messageTypeLen + 2 + channelNameLen + 2 + producer_idLen + 2 + agentNameLen + 8 + 8 + len(bodyBB)

					var byteSendBuffer = ByteBuffer.Buffer{
						Endian:"big",
					}

					byteSendBuffer.PutLong(newTotalByteLen) // total packet length

					byteSendBuffer.PutShort(messageTypeLen) // total message type length

					byteSendBuffer.Put([]byte(messageType)) // message type value

					byteSendBuffer.PutShort(channelNameLen) // total channel name length

					byteSendBuffer.Put([]byte(channelName)) // channel name value

					byteSendBuffer.PutShort(producer_idLen) // producerid length

					byteSendBuffer.Put([]byte(producer_id)) // producerid value

					byteSendBuffer.PutShort(agentNameLen) // agentName length

					byteSendBuffer.Put([]byte(agentName)) // agentName value

					byteSendBuffer.PutLong(int(id)) // backend offset

					byteSendBuffer.PutLong(int(cursor)) // total bytes subscriber packet received

					byteSendBuffer.Put(bodyBB) // actual body

					go send(conn, byteSendBuffer, sentMsg)

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
	
}

func send(conn net.TCPConn, packetBuffer ByteBuffer.Buffer, sentMsg chan bool){ 

	defer ChannelList.Recover()

	var totalRetry = 0

	RETRY:

	totalRetry += 1

	if totalRetry > 5{

		sentMsg <- false

		return

	}

	_, err := conn.Write(packetBuffer.Array())
	
	if err != nil {
	
		go ChannelList.WriteLog(err.Error())

		time.Sleep(1 * time.Second)

		goto RETRY

	}

	sentMsg <- true

}

func (e *ChannelMethods) SendAck(messageMap pojo.PacketStruct, ackChan chan bool){

	defer ChannelList.Recover()

	var byteBuffer = ByteBuffer.Buffer{
		Endian:"big",
	}

	byteBuffer.PutLong(len(messageMap.Producer_id))

	byteBuffer.Put([]byte(messageMap.Producer_id))

	_, err := messageMap.Conn.Write(byteBuffer.Array())

	if err != nil{

		go ChannelList.WriteLog(err.Error())

		ackChan <- false

		return
	}

	ackChan <- true

}