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

// 

func (e *CreateProperties) receiveSubMsg(conn net.Conn){

	defer handlepanic()

	var callbackChan = make(chan string, 1)
	defer close(callbackChan)

	if e.ConnectionType == "tcp"{

		for {	

			sizeBuf := make([]byte, 8)

			conn.Read(sizeBuf)

			packetSize := binary.BigEndian.Uint64(sizeBuf)

			if packetSize < 0 {
				continue
			}

			statusBuf := make([]byte, 1)

			conn.Read(statusBuf)

			completePacket := make([]byte, packetSize)

			conn.Read(completePacket)

			if allZero(completePacket) {

				break
			}

			if statusBuf[0] == 1{

				panic(string(completePacket))

				break

			}

			if e.ReadDelay > 0{
				time.Sleep(time.Duration(e.ReadDelay) * time.Nanosecond)
			}

			go e.parseMsg(int64(packetSize), completePacket, "sub", callbackChan)

			<-callbackChan

		}

	}

	go log.Println("Socket disconnected...")

	conn.Close()

	e.connectStatus = false
}

func (e *CreateProperties) receiveMsg(conn net.Conn){

	defer handlepanic()

	var callbackChan = make(chan string, 1)
	defer close(callbackChan)

	for {	

		sizeBuf := make([]byte, 8)

		conn.Read(sizeBuf)

		packetSize := binary.BigEndian.Uint64(sizeBuf)

		if packetSize < 0 {
			continue
		}

		statusBuf := make([]byte, 1)

		conn.Read(statusBuf)

		var completePacket []byte

		completePacket = make([]byte, packetSize)

		conn.Read(completePacket)

		if allZero(completePacket) {

			break
		}

		if statusBuf[0] == 1{

			panic(string(completePacket))

			break

		}

		go e.parseMsg(int64(packetSize), completePacket, "pub", callbackChan)

		select {

			case message, ok := <-callbackChan:	

				if ok{

					if message != "REJECT" && message != "SUCCESS"{
						e.Lock()
						delete(e.TransactionList, message)
						e.Unlock()
					}

				}
			break
		}
		
	}

	go log.Println("Socket disconnected...")

	conn.Close()

	e.connectStatus = false
}