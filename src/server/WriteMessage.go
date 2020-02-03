package server

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
)

func WriteBytes(conn net.Conn, status int, message []byte) {

	// buffer memory for packet size

	var packetBuffer bytes.Buffer

	sizeBuff := make([]byte, 4)

	binary.LittleEndian.PutUint32(sizeBuff, uint32(len(message)+2))

	packetBuffer.Write(sizeBuff)

	// buffer memory for status

	statusBuff := make([]byte, 2)

	binary.LittleEndian.PutUint16(statusBuff, uint16(status))

	packetBuffer.Write(statusBuff)

	// message in byte array

	packetBuffer.Write(message)

	_, err := conn.Write(packetBuffer.Bytes())

	if err != nil {
		fmt.Println("Error writing to stream." + err.Error())
	}
}
