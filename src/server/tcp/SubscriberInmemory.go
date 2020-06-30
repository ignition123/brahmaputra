package tcp

import(
	"ChannelList"
	"pojo"
	"ByteBuffer"
	_"time"
	_"log"
)

func SubscriberInmemory(clientObj *pojo.ClientObject){

	defer ChannelList.Recover()

	exitLoop:

		for msg := range clientObj.Channel{

			if msg == nil || !sendMessageToClient(clientObj, msg){

				break exitLoop

			}

		}

	clientObj.Conn.Close()

	go ChannelList.WriteLog("Socket subscriber client closed...")

}

func createBufferPacket(message *pojo.PacketStruct) []byte{

	defer ChannelList.Recover()

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

	return byteBuffer.Array()
}

func sendMessageToClient(clientObj *pojo.ClientObject, message *pojo.PublishMsg) bool{

	defer ChannelList.Recover()

	_, err := clientObj.Conn.Write(message.Msg)
		
	if err != nil {

		go ChannelList.WriteLog(err.Error())

		return false

	}

	return true

}