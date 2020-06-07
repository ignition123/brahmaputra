package tcp

/*
	File contains methods writing error to the subscriber or publisher
*/

// importing modules

import(
	"net"
	"ChannelList"
	"ByteBuffer"
	"pojo"
)

// method throwing error to individual subscriber or publisher

func throughClientError(conn net.TCPConn, message string){

	defer ChannelList.Recover()

	totalByteLen := len(message)

	byteSendBuffer := ByteBuffer.Buffer{
		Endian:"big",
	}

	byteSendBuffer.PutLong(totalByteLen) // total packet length

	byteSendBuffer.PutByte(byte(1)) // status code

	byteSendBuffer.Put([]byte(message)) // actual body

	_, err := conn.Write(byteSendBuffer.Array())

	if (err != nil){

		go ChannelList.WriteLog(err.Error())

	}

	go ChannelList.WriteLog(message)

	conn.Close()
}

// method throwing error to subscriber group

func throughGroupError(channelName string, groupName string, message string){

	defer ChannelList.Recover()

	ChannelList.TCPStorage[channelName].ChannelLock.Lock()
	defer ChannelList.TCPStorage[channelName].ChannelLock.Unlock()

	totalByteLen := len(message)

	byteSendBuffer := ByteBuffer.Buffer{
		Endian:"big",
	}

	byteSendBuffer.PutLong(totalByteLen) // total packet length

	byteSendBuffer.PutByte(byte(1)) // status code

	byteSendBuffer.Put([]byte(message)) // actual body

	groupLen := len(ChannelList.TCPStorage[channelName].Group[groupName])

	for i:=0;i<groupLen;i++{

		groupObj := ChannelList.TCPStorage[channelName].Group[groupName][i]

		_, err := groupObj.Conn.Write(byteSendBuffer.Array())
		
		if (err != nil){

			go ChannelList.WriteLog(err.Error())

		}

		groupPacket := ChannelList.TCPStorage[channelName].Group[groupName][i]

		subscriberName := groupPacket.ChannelName+groupPacket.SubscriberName+groupPacket.GroupName

		delete(ChannelList.TCPStorage[channelName].SubscriberList, subscriberName)	

		groupObj.Conn.Close()

	}

	var newList []*pojo.PacketStruct

	ChannelList.TCPStorage[channelName].Group[groupName] = newList

	go ChannelList.WriteLog(message)

}