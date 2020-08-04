package tcp

import(
	"ByteBuffer"
	"objects"
	"ChannelList"
)

func SendFinPacket(clientObj *objects.ClientObject){

	defer ChannelList.Recover()

	messageType := "FIN"

	messageTypeLen := len(messageType)

	totalPacketLen := 2 + messageTypeLen

	// creating bytebuffer of big endian and setting the values to be published

	byteSendBuffer := ByteBuffer.Buffer{
		Endian:"big",
	}

	byteSendBuffer.PutLong(totalPacketLen) // total packet length

	byteSendBuffer.PutByte(byte(2)) // status code

	byteSendBuffer.PutShort(messageTypeLen)

	byteSendBuffer.Put([]byte(messageType))

	_, err := clientObj.Conn.Write(byteSendBuffer.Array())
	
	if err != nil {
	
		go ChannelList.WriteLog(err.Error())
	}
}