package brahmaputra

/*
	Subscribe method, here the publisher receives the ack and subscriber receives the packets
*/

// import modules

import(
	"encoding/binary"
	"net"
	"log"
	"time"
	"io"
	"bufio"
)

// method to check if the packet is blank

func allZero(s []byte) bool {

	defer handlepanic()

	for _, v := range s {

		if v != 0 {

			return false

		}

	}

	return true
}

// method receiving the subscriber messages, listening to socket

func (e *CreateProperties) receiveSubMsg(conn net.Conn){

	defer handlepanic()

	// callback boolean channel

	callbackChan := make(chan string, 1)
	defer close(callbackChan)

	bufferReader := bufio.NewReader(conn)

	// checking the connection Type

	if e.ConnectionType == "tcp"{

		// iterating infinitely listening to tcp sockets

		for {

			// read timeout

			if e.TCP.SocketReadTimeout != 0{
				conn.(*net.TCPConn).SetReadDeadline(time.Now().Add(time.Duration(e.TCP.SocketReadTimeout) * time.Millisecond))
			}

			// creating a byte array of 8 byte size	

			sizeBuf := make([]byte, 8)

			_, err := io.ReadAtLeast(bufferReader, sizeBuf[:int(8)], 8)

			// reading from tcp socket getting the size of the total packet

			if err == io.EOF{
			
				log.Println("Connection closed...")

				break

			}

			// converting the sizeBuf into int64

			packetSize := binary.BigEndian.Uint64(sizeBuf)

			if packetSize < 0 {
				continue
			}

			// creating a byte array of 1 size to get the status of the packet

			statusBuf := make([]byte, 1)

			_, err = io.ReadAtLeast(bufferReader, sizeBuf[:int(8)], 8)

			// reading from tcp socket getting the size of the total packet

			if err == io.EOF{
			
				log.Println("Connection closed...")

				break

			}

			// creating a byte array of packet size

			completePacket := make([]byte, packetSize)

			// reading the complete packet

			_, err = io.ReadAtLeast(bufferReader, completePacket[:int(packetSize)], int(packetSize))

			// reading from tcp socket getting the size of the total packet

			if err == io.EOF{
			
				log.Println("Connection closed...")

				break

			}

			// checking if the packet is a blank array

			if allZero(completePacket) {

				break
			}

			// checking the status of the message

			if statusBuf[0] == 1{

				panic(string(completePacket))

				break

			}

			// checking if there is any read delay

			if e.ReadDelay > 0{
				time.Sleep(time.Duration(e.ReadDelay) * time.Nanosecond)
			}

			// sending the message to the parse method to parse the complete packet

			go e.parseMsg(int64(packetSize), completePacket, "sub", callbackChan)

			// waiting for callback

			<-callbackChan

		}

	}

	// socket disconnection closing the tcp socket

	go log.Println("Socket disconnected...")

	conn.Close()

	// changing the connection status boolean variable

	e.connectStatus = false
}

// method for producer to receive the acknowledgement

func (e *CreateProperties) receiveMsg(conn net.Conn){

	defer handlepanic()

	// creating the callback channel

	callbackChan := make(chan string, 1)
	defer close(callbackChan)

	bufferReader := bufio.NewReader(conn)

	// listening to sockets

	for {	

		// read timeout

		if e.TCP.SocketReadTimeout != 0{
			conn.(*net.TCPConn).SetReadDeadline(time.Now().Add(time.Duration(e.TCP.SocketReadTimeout) * time.Millisecond))
		}

		// creating a empty byte array of size 8 to get the total size of the packet

		sizeBuf := make([]byte, 8)

		// reading 8 bytes from tcp sockets

		_, err := io.ReadAtLeast(bufferReader, sizeBuf[:int(8)], 8)

		// reading from tcp socket getting the size of the total packet

		if err == io.EOF{
		
			log.Println("Connection closed...")

			break

		}

		// changing the byte array to int64

		packetSize := binary.BigEndian.Uint64(sizeBuf)

		// checking if packet size is less then zero

		if packetSize < 0 {
			continue
		}

		// creating a byte array of 1 size to get the status of the packet

		statusBuf := make([]byte, 1)

		_, err = io.ReadAtLeast(bufferReader, sizeBuf[:int(1)], 1)

		// reading from tcp socket getting the size of the total packet

		if err == io.EOF{
		
			log.Println("Connection closed...")

			break

		}

		// creating byte array of packetSize

		completePacket := make([]byte, packetSize)

		// reading the entire packet from tcp pipe

		_, err = io.ReadAtLeast(bufferReader, completePacket[:int(packetSize)], int(packetSize))

		// reading from tcp socket getting the size of the total packet

		if err == io.EOF{
		
			log.Println("Connection closed...")

			break

		}

		// checking if the packet has commplete blank byte array

		if allZero(completePacket) {

			break
		}

		// checking the status of the message packet 1 means error and 2 means success

		if statusBuf[0] == 1{

			panic(string(completePacket))

			break

		}

		// passing the message to the parse message to parse the acknowledgment message

		go e.parseMsg(int64(packetSize), completePacket, "pub", callbackChan)

		// waiting for callback

		message, ok := <-callbackChan

		if ok{

			// deleting the message from the transaction list

			e.Lock()
			delete(e.TransactionList, message)
			e.Unlock()

		}

	}

	go log.Println("Socket disconnected...")

	conn.Close()

	e.connectStatus = false
}