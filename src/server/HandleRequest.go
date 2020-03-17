package server

import (
	"encoding/binary"
	"net"
	"time"
	"ChannelList"
	"Utilization"
	"sync"
)

var closeTCP = false

func allZero(s []byte) bool {

	defer ChannelList.Recover()
	
	for _, v := range s {
		if v != 0 {
			return false
		}
	}
	return true
}

func HandleRequest(conn net.TCPConn) {
	
	defer ChannelList.Recover()

	var waitgroup sync.WaitGroup

	for {

		if closeTCP{
			go ChannelList.WriteLog("Closing all current sockets...")
			conn.Close()
			break
		}
		
		sizeBuf := make([]byte, 4)

		conn.Read(sizeBuf)

		packetSize := binary.LittleEndian.Uint32(sizeBuf)

		if packetSize < 0 {
			continue
		}

		completePacket := make([]byte, packetSize)

		conn.Read(completePacket)

		if allZero(completePacket) {
			go ChannelList.WriteLog("Connection closed...")
			break
		}

		var message = string(completePacket)

		waitgroup.Add(1)

		go ParseMsg(message, conn, &waitgroup)

		waitgroup.Wait()
	}

	conn.Close()
}

func ShowUtilization(){
	for{

		if !closeTCP{
			time.Sleep(5 * time.Second)
			Utilization.GetHardwareData()
		}else{
			break
		}

	}
}

func CloseTCPServers(){
	
	defer ChannelList.Recover()

	ChannelList.WriteLog("Closing tcp socket...")

	closeTCP = true
}