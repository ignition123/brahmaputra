package ChannelList

/*
	File contains methods writing error to the subscriber or publisher
*/

// importing modules

import(
	"net"
	"ByteBuffer"
	"pojo"
)

// method throwing error to individual subscriber or publisher

func ThroughClientError(conn net.TCPConn, message string){

	defer Recover()

	totalByteLen := len(message)

	byteSendBuffer := ByteBuffer.Buffer{
		Endian:"big",
	}

	byteSendBuffer.PutLong(totalByteLen) // total packet length

	byteSendBuffer.PutByte(byte(1)) // status code

	byteSendBuffer.Put([]byte(message)) // actual body

	_, err := conn.Write(byteSendBuffer.Array())

	if (err != nil){

		go WriteLog(err.Error())

	}

	go WriteLog(message)

	conn.Close()
}

// method throwing error to subscriber group

func ThroughGroupError(channelName string, groupName string, message string){

	defer Recover()

	pojo.SubscriberObj[channelName].Channel.ChannelLock.Lock()
	defer pojo.SubscriberObj[channelName].Channel.ChannelLock.Unlock()

	totalByteLen := len(message)

	byteSendBuffer := ByteBuffer.Buffer{
		Endian:"big",
	}

	byteSendBuffer.PutLong(totalByteLen) // total packet length

	byteSendBuffer.PutByte(byte(1)) // status code

	byteSendBuffer.Put([]byte(message)) // actual body

	for index :=  range pojo.SubscriberObj[channelName].Groups[groupName]{

		_, err := pojo.SubscriberObj[channelName].Groups[groupName][index].Conn.Write(byteSendBuffer.Array())
		
		if (err != nil){

			go WriteLog(err.Error())

		}

		pojo.SubscriberObj[channelName].UnRegister <- pojo.SubscriberObj[channelName].Groups[groupName][index]

	}

	go WriteLog(message)

}

// Producer Acknowledgement method

func SendAck(messageMap *pojo.PacketStruct, clientObj *pojo.ClientObject){

	defer Recover()

	// creating byte buffer to send acknowledgement to producer

	byteBuffer := ByteBuffer.Buffer{
		Endian:"big",
	}

	byteBuffer.PutLong(len(messageMap.Producer_id))

	byteBuffer.PutByte(byte(2)) // status code

	byteBuffer.Put([]byte(messageMap.Producer_id))

	// writing to tcp socket

	_, err := clientObj.Conn.Write(byteBuffer.Array())

	if err != nil{

		go WriteLog(err.Error())

		return
	}

}
