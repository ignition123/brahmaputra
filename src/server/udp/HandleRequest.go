package udp


import (
	"encoding/binary"
	"net"
	"ChannelList"
	"log"
)

var closeUDP = false

func allZero(s []byte) bool {

	defer ChannelList.Recover()
	
	for _, v := range s {
		if v != 0 {
			return false
		}
	}
	return true
}

func HandleRequest(conn *net.UDPConn){

	defer ChannelList.Recover()

	parseChan := make(chan bool, 1)

	var writeCount = 0

	var subscriberMapName string

	var channelMapName string

	var messageMapType string

	var groupMapName = ""

	for {

		if closeUDP{
			go ChannelList.WriteLog("Closing all current sockets...")
			break
		}

		sizeBuf := make([]byte, 1024)

		_, addr, err := conn.ReadFromUDP(sizeBuf)

		if err != nil{

			log.Println(err)

			continue
		}

		packetSize := binary.BigEndian.Uint64(sizeBuf[:8])

		if packetSize < 0 {
			continue
		}

		if allZero(sizeBuf) {

			go ChannelList.WriteLog("Connection closed...")
			break
		}

		sizeBuf = sizeBuf[8:(packetSize + 8)]

		if ChannelList.UDPStorage[channelMapName] != nil{

			if writeCount >= ChannelList.UDPStorage[channelMapName].PartitionCount{

				writeCount = 0

			}

		}else{

			writeCount = 0
		}

		go ParseMsg(int64(packetSize), sizeBuf, *conn, addr, parseChan, writeCount, &subscriberMapName, &channelMapName, &messageMapType, &groupMapName)

		writeCount += 1

		<-parseChan
	}

	if messageMapType == "subscribe"{

		DeleteUDPChannelSubscriberList(channelMapName, subscriberMapName)

		if groupMapName != ""{

			//get consumer group Length

			var consumerGroupLen = GetChannelGrpMapLen(channelMapName, groupMapName)

			if consumerGroupLen > 0{

				// Delete Group Member

				RemoveGroupMember(channelMapName, groupMapName, subscriberMapName)

			}
		}
	}

}

func CloseUDPServers(){
	
	defer ChannelList.Recover()

	ChannelList.WriteLog("Closing udp socket...")

	closeUDP = true
}