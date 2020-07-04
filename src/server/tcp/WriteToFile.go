package tcp

import(
	"objects"
	"ChannelList"
	"ByteBuffer"
)

// writing data to file, in append mode

func WriteData(packet objects.PacketStruct, writeCount *int, clientObj *objects.ClientObject) bool{

	defer ChannelList.Recover()

	byteBuffer := ByteBuffer.Buffer{
		Endian:"big",
	}

	// totalLen + messageTypelen + messageType + channelNameLen + channelName + producerIdLen + producerID + agentNameLen + agentName + _id + compressionType + totalBytePacket

	totalByteLen := 2 + packet.MessageTypeLen + 2 + packet.ChannelNameLen + 2 + packet.Producer_idLen + 2 + packet.AgentNameLen + 8 + 1 + len(packet.BodyBB)

	byteBuffer.PutLong(totalByteLen)

	byteBuffer.PutShort(packet.MessageTypeLen)

	byteBuffer.Put([]byte(packet.MessageType))

	byteBuffer.PutShort(packet.ChannelNameLen)

	byteBuffer.Put([]byte(packet.ChannelName))

	byteBuffer.PutShort(packet.Producer_idLen)

	byteBuffer.Put([]byte(packet.Producer_id))

	byteBuffer.PutShort(packet.AgentNameLen)

	byteBuffer.Put([]byte(packet.AgentName))

	byteBuffer.PutLong(int(packet.Id))

	byteBuffer.PutByte(packet.CompressionType)

	byteBuffer.Put(packet.BodyBB)

	_, err := objects.SubscriberObj[packet.ChannelName].Channel.FD[*writeCount].Write(byteBuffer.Array())


	if (err != nil){
		
		go ChannelList.WriteLog(err.Error())
		
		return false
	}

	return true
}