package server

import(
	"net"
	"ChannelList"
	"ByteBuffer"
	"pojo"
)

func ThroughClientError(conn net.TCPConn, message string){

	defer ChannelList.Recover()

	var totalByteLen = len(message)

	var byteSendBuffer = ByteBuffer.Buffer{
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

func ThroughGroupError(channelName string, groupName string, message string){

	defer ChannelList.Recover()

	ChannelList.TCPStorage[channelName].ChannelLock.Lock()
	defer ChannelList.TCPStorage[channelName].ChannelLock.Unlock()

	var totalByteLen = len(message)

	var byteSendBuffer = ByteBuffer.Buffer{
		Endian:"big",
	}

	byteSendBuffer.PutLong(totalByteLen) // total packet length

	byteSendBuffer.PutByte(byte(1)) // status code

	byteSendBuffer.Put([]byte(message)) // actual body

	var groupLen = len(ChannelList.TCPStorage[channelName].Group[groupName])

	for i:=0;i<groupLen;i++{

		var groupObj = ChannelList.TCPStorage[channelName].Group[groupName][i]

		_, err := groupObj.Conn.Write(byteSendBuffer.Array())
		
		if (err != nil){

			go ChannelList.WriteLog(err.Error())

		}

		var groupPacket = ChannelList.TCPStorage[channelName].Group[groupName][i]

		var subscriberName = groupPacket.ChannelName+groupPacket.SubscriberName+groupPacket.GroupName

		delete(ChannelList.TCPStorage[channelName].SubscriberList, subscriberName)	

		groupObj.Conn.Close()

	}

	var newList []*pojo.PacketStruct

	ChannelList.TCPStorage[channelName].Group[groupName] = newList

	go ChannelList.WriteLog(message)

}