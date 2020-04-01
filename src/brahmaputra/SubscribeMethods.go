package brahmaputra

import(
	"encoding/binary"
	"net"
	"log"
	"time"
)

func (e *CreateProperties) receiveSubMsg(conn net.Conn){

	defer handlepanic()

	var callbackChan = make(chan string, 1)

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

	}else if e.ConnectionType == "udp"{

		for{

			sizeBuf := make([]byte, 1024)

			conn.Read(sizeBuf)

			packetSize := binary.BigEndian.Uint64(sizeBuf[:9])

			if packetSize < 0 {
				continue
			}

			if allZero(sizeBuf) {

				break
			}

			var statusBuf = sizeBuf[8:9]

			sizeBuf = sizeBuf[9:(packetSize + 9)]

			if statusBuf[0] == 1{

				panic(string(sizeBuf))

				break

			}

			if e.ReadDelay > 0{
				time.Sleep(time.Duration(e.ReadDelay) * time.Nanosecond)
			}

			go e.parseMsg(int64(packetSize), sizeBuf, "sub", callbackChan)

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