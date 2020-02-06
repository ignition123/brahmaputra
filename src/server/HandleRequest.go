package server

import (
	"encoding/binary"
	"net"
	"time"
)

func allZero(s []byte) bool {
	for _, v := range s {
		if v != 0 {
			return false
		}
	}
	return true
}

func HandleRequest(conn net.Conn, messageQueue chan string) {

	defer conn.Close()

	sizeBuf := make([]byte, 4)

	for {
		
		conn.Read(sizeBuf)

		packetSize := binary.LittleEndian.Uint32(sizeBuf)

		if packetSize < 0 {
			continue
		}

		completePacket := make([]byte, packetSize)

		conn.Read(completePacket)

		if allZero(completePacket) {
			messageQueue <- "BRAHMAPUTRA_DISCONNECT"
			break
		}

		var message = string(completePacket)

		err := conn.SetReadDeadline(time.Now().Add(10 * time.Hour))

		if err != nil {
			go WriteLog("Error in tcp connection: " + err.Error())
			messageQueue <- "BRAHMAPUTRA_DISCONNECT"
			break
		}

		if len(message) > 0 {
			messageQueue <- message
		}
	}

	close(messageQueue)
}
