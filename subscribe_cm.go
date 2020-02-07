/*
A very simple TCP client written in Go.
This is a toy project that I used to learn the fundamentals of writing
Go code and doing some really basic network stuff.
Maybe it will be fun for you to read. It's not meant to be
particularly idiomatic, or well-written for that matter.
*/
package main

import (
	_"bufio"
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"net"
	"os"
	_ "regexp"
	"strconv"
	_"strings"
	"time"
	"encoding/json"
)

var host = flag.String("host", "localhost", "The hostname or IP to connect to; defaults to \"localhost\".")
var port = flag.Int("port", 8100, "The port to connect to; defaults to 8000.")

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

	conn.(*net.TCPConn).SetKeepAlive(true)
	conn.(*net.TCPConn).SetKeepAlive(true)
	conn.(*net.TCPConn).SetLinger(1)
	conn.(*net.TCPConn).SetNoDelay(true)
	conn.(*net.TCPConn).SetReadBuffer(10000)
	conn.(*net.TCPConn).SetWriteBuffer(10000)
	conn.(*net.TCPConn).SetDeadline(time.Now().Add(1000000 * time.Second))
	conn.(*net.TCPConn).SetReadDeadline(time.Now().Add(1000000 * time.Second))
	conn.(*net.TCPConn).SetWriteDeadline(time.Now().Add(1000000 * time.Second))

	time.Sleep(10)

	// currentTime := time.Now()

	var packetBuffer bytes.Buffer

	buff := make([]byte, 4)

	// var _id = strconv.FormatInt(currentTime.UnixNano(), 10)

	var messageMap = make(map[string]interface{})

	var cm = make(map[string]interface{})
	cm["Exchange"] = "NSE"
	cm["Segment"] = "CM"

	messageMap["contentMatcher"] = cm
	messageMap["channelName"] = "Abhik"
	messageMap["type"] = "subscribe"

	jsonData, err := json.Marshal(messageMap)

	fmt.Println(string(jsonData))

	if err != nil{

		fmt.Println(err)
		return

	}

	binary.LittleEndian.PutUint32(buff, uint32(len(jsonData)))

	packetBuffer.Write(buff)

	packetBuffer.Write(jsonData)

	conn.SetWriteDeadline(time.Now().Add(1 * time.Second))

	fmt.Println(time.Now())
	_, err = conn.Write(packetBuffer.Bytes())

	// break

	if err != nil {
		fmt.Println("Error writing to stream." + err.Error())
	}

	readConnection(conn)
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

	for {

		sizeBuf := make([]byte, 4)

		conn.Read(sizeBuf)

		packetSize := binary.LittleEndian.Uint32(sizeBuf)

		if packetSize < 0 {
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

		fmt.Println(message)
		writeLog(message)
	}

	conn.Close()
}

func writeLog(logMessage string){
	ErrorFile, err := os.OpenFile("./subscribe.log", os.O_APPEND|os.O_WRONLY, 0600)

	if err != nil {
		fmt.Println(err)
		return
	}

	defer ErrorFile.Close()
	ErrorFile.WriteString(logMessage)
}