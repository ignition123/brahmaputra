package udp

import(
	"net"
	"ChannelList"
	"ByteBuffer"
	"pojo"
)

func ThroughUDPClientError(conn net.UDPConn, message string){

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

}

func ThroughGroupError(channelName string, groupName string, message string){

	defer ChannelList.Recover()

	ChannelList.UDPStorage[channelName].ChannelLock.Lock()
	defer ChannelList.UDPStorage[channelName].ChannelLock.Unlock()

	var totalByteLen = len(message)

	var byteSendBuffer = ByteBuffer.Buffer{
		Endian:"big",
	}

	byteSendBuffer.PutLong(totalByteLen) // total packet length

	byteSendBuffer.PutByte(byte(1)) // status code

	byteSendBuffer.Put([]byte(message)) // actual body

	var groupLen = len(ChannelList.UDPStorage[channelName].Group[groupName])

	for i:=0;i<groupLen;i++{

		var groupObj = ChannelList.UDPStorage[channelName].Group[groupName][i]

		_, err := groupObj.Conn.Write(byteSendBuffer.Array())
		
		if (err != nil){

			go ChannelList.WriteLog(err.Error())

		}

		var groupPacket = ChannelList.UDPStorage[channelName].Group[groupName][i]

		var subscriberName = groupPacket.ChannelName+groupPacket.SubscriberName+groupPacket.GroupName

		delete(ChannelList.UDPStorage[channelName].SubscriberList, subscriberName)

	}

	var newList []*pojo.UDPPacketStruct

	ChannelList.UDPStorage[channelName].Group[groupName] = newList

	go ChannelList.WriteLog(message)

}