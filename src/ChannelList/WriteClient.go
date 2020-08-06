package ChannelList

/*
	File contains methods writing error to the subscriber or publisher
*/

// importing modules

import(
	"net"
	"ByteBuffer"
	"objects"
	"time"
)

// method throwing error to individual subscriber or publisher

func ThroughClientError(conn net.Conn, message string){

	defer Recover()

	totalByteLen := len(message)

	byteSendBuffer := ByteBuffer.Buffer{
		Endian:"big",
	}

	byteSendBuffer.PutLong(totalByteLen) // total packet length

	byteSendBuffer.PutByte(byte(1)) // status code

	byteSendBuffer.Put([]byte(message)) // actual body

	if *ConfigTCPObj.Server.TCP.SocketWriteTimeout != 0{

		conn.(*net.TCPConn).SetWriteDeadline(time.Now().Add(time.Duration(*ConfigTCPObj.Server.TCP.SocketWriteTimeout) * time.Millisecond))

	}else{

		conn.(*net.TCPConn).SetWriteDeadline(time.Time{})
	}

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

	totalByteLen := len(message)

	byteSendBuffer := ByteBuffer.Buffer{
		Endian:"big",
	}

	byteSendBuffer.PutLong(totalByteLen) // total packet length

	byteSendBuffer.PutByte(byte(1)) // status code

	byteSendBuffer.Put([]byte(message)) // actual body

	channelLock.Lock()

	for index :=  range objects.SubscriberObj[channelName].Groups[groupName]{

		if *ConfigTCPObj.Server.TCP.SocketWriteTimeout != 0{

			objects.SubscriberObj[channelName].Groups[groupName][index].Conn.(*net.TCPConn).SetWriteDeadline(time.Now().Add(time.Duration(*ConfigTCPObj.Server.TCP.SocketWriteTimeout) * time.Millisecond))

		}else{

			objects.SubscriberObj[channelName].Groups[groupName][index].Conn.(*net.TCPConn).SetWriteDeadline(time.Time{})
		}

		_, err := objects.SubscriberObj[channelName].Groups[groupName][index].Conn.Write(byteSendBuffer.Array())
		
		if (err != nil){

			go WriteLog(err.Error())

		}

		objects.SubscriberObj[channelName].UnRegister <- objects.SubscriberObj[channelName].Groups[groupName][index]

	}
	
	channelLock.Unlock()

	go WriteLog(message)

}

// Producer Acknowledgement method

func SendAck(messageMap *objects.PacketStruct, clientObj *objects.ClientObject){

	defer Recover()

	// creating byte buffer to send acknowledgement to producer

	byteBuffer := ByteBuffer.Buffer{
		Endian:"big",
	}

	byteBuffer.PutLong(len(messageMap.Producer_id))

	byteBuffer.PutByte(byte(2)) // status code

	byteBuffer.Put([]byte(messageMap.Producer_id))

	// writing to tcp socket

	if *ConfigTCPObj.Server.TCP.SocketWriteTimeout != 0{

		clientObj.Conn.(*net.TCPConn).SetWriteDeadline(time.Now().Add(time.Duration(*ConfigTCPObj.Server.TCP.SocketWriteTimeout) * time.Millisecond))

	}else{

		clientObj.Conn.(*net.TCPConn).SetWriteDeadline(time.Time{})
	}

	_, err := clientObj.Conn.Write(byteBuffer.Array())

	if err != nil{

		go WriteLog(err.Error())

		return
	}

}
