/*
A very simple TCP client written in Go.
This is a toy project that I used to learn the fundamentals of writing
Go code and doing some really basic network stuff.
Maybe it will be fun for you to read. It's not meant to be
particularly idiomatic, or well-written for that matter.
*/
package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"net"
	"os"
	_ "regexp"
	"strconv"
	"strings"
	"time"
)

var host = flag.String("host", "localhost", "The hostname or IP to connect to; defaults to \"localhost\".")
var port = flag.Int("port", 8900, "The port to connect to; defaults to 8000.")

func main() {
	flag.Parse()

	dest := *host + ":" + strconv.Itoa(*port)
	fmt.Printf("Connecting to %s...\n", dest)

	conn, err := net.Dial("tcp", dest)

	if err != nil {
		if _, t := err.(*net.OpError); t {
			fmt.Println("Some problem connecting.")
		} else {
			fmt.Println("Unknown error: " + err.Error())
		}
		os.Exit(1)
	}

	go readConnection(conn)

	fmt.Print("127.0.0.1:8900>")

	for {

		time.Sleep(10)

		reader := bufio.NewReader(os.Stdin)

		text, _ := reader.ReadString('\n')

		text = strings.TrimRight(text, "\r\n")

		if text == "" {
			fmt.Print("127.0.0.1:8900>")
			continue
		}

		var packetBuffer bytes.Buffer

		buff := make([]byte, 4)

		binary.LittleEndian.PutUint32(buff, uint32(len(text)))

		packetBuffer.Write(buff)

		packetBuffer.Write([]byte(text))

		conn.SetWriteDeadline(time.Now().Add(1 * time.Second))

		_, err := conn.Write(packetBuffer.Bytes())

		if err != nil {
			fmt.Println("Error writing to stream." + err.Error())
			os.Exit(1)
			break
		}
	}
}

func allZero(s []byte) bool {
	for _, v := range s {
		if v != 0 {
			return false
		}
	}
	return true
}

func readConnection(conn net.Conn) {

	sizeBuf := make([]byte, 4)

	statusBuf := make([]byte, 2)

	for {

		time.Sleep(1)

		conn.Read(sizeBuf)

		packetSize := binary.LittleEndian.Uint32(sizeBuf)

		if packetSize < 0 {
			continue
		}

		conn.Read(statusBuf)

		packetStatus := binary.LittleEndian.Uint16(statusBuf)

		if packetStatus != 1 && packetStatus != 2 {
			continue
		}

		completePacket := make([]byte, packetSize)

		conn.Read(completePacket)

		if allZero(completePacket) {
			fmt.Println("Server disconnected")
			os.Exit(1)
			break
		}

		var message = string(completePacket)

		if packetStatus == 1 {
			fmt.Println(message)
		} else {
			fmt.Println("Exception: " + message)
		}

		fmt.Print("127.0.0.1:8900>")
	}

	conn.Close()
}
