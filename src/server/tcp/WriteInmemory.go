package tcp

import(
	"pojo"
	"ChannelList"
	"ByteBuffer"
)

// sending to all clients that are connected to inmemory channels

func (e *ChannelMethods) sendMessageToClient(message pojo.PacketStruct, msgChan chan bool){

	defer ChannelList.Recover()

	// getting the client list this method uses mutex to prevent race condition

	channelSockList := getClientListInmemory(message.ChannelName)

	// iterating over the hashmap

	for key, _ := range channelSockList{

		// creating bytebuffer

		byteBuffer := ByteBuffer.Buffer{
			Endian:"big",
		}

		// creating total length of the byte buffer

		// MessageTypeLen + messageType + ChannelNameLength + channelname + producer_idLen + producer_id + AgentNameLen + AgentName + backendOffset + compressionType + actualBody

		totalByteLen := 2 + message.MessageTypeLen + 2 + message.ChannelNameLen + 2 + message.Producer_idLen + 2 + message.AgentNameLen + 8 + 1 + len(message.BodyBB)

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

		// byteBuffer.PutLong(0) // total bytes subscriber packet received

		byteBuffer.PutByte(message.CompressionType) // compression type (zlib, gzip, snappy, lz4)

		byteBuffer.Put(message.BodyBB) // actual body

		e.sendInMemory(message, key, byteBuffer)
	}

	msgChan <- true
}

// sending Inmemory client

func (e *ChannelMethods) sendInMemory(message pojo.PacketStruct, key string, packetBuffer ByteBuffer.Buffer){ 

	defer ChannelList.Recover()

	// getting the length of the socket client connected

	stat, sock := findInmemorySocketListLength(message.ChannelName, key)

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