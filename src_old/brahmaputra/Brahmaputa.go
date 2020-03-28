package brahmaputra

import(
	"log"
	"net"
	"encoding/binary"
	"time"
	"strconv"
	"os"
	"sync"
	"runtime"
	"ByteBuffer"
	"encoding/json"
	"net/url"
)

var SubscriberChannel = make(chan interface{}, 1)

type CreateProperties struct{
	Url string
	AuthToken string
	ConnectionType string
	Conn net.Conn
	ChannelName string
	AgentName string
	TransactionList map[string][]byte
	AppType string
	autoIncr int64
	Worker int
	sync.Mutex
	ConnPool []net.Conn
	PoolSize int
	roundRobin int
	requestPull [][]byte
	connectStatus bool
	requestWg sync.WaitGroup
	ReceiveSync sync.WaitGroup
	requestChan chan bool
	contentMatcher string
	contentMatcherMap map[string]interface{}
	AlwaysStartFrom string
	subscribeFD *os.File
	WriteDelay int32
	ReadDelay int32
	GroupName string
	SubscriberName string
	AuthReconnect bool
	Acknowledge bool
}	

func handlepanic() { 
  
    if a := recover(); a != nil { 
        log.Println(a)
    } 
} 

func (e *CreateProperties) Connect(){

	defer handlepanic()

	if e.AppType != "producer" && e.AppType != "consumer"{

		go log.Println("AppType must be producer or consumer...")

		return

	}

	var subReconnect = false

	if e.Acknowledge{

		e.TransactionList = make(map[string][]byte)
		
	}

	e.contentMatcherMap = make(map[string]interface{})

	if e.AlwaysStartFrom == ""{

		e.AlwaysStartFrom = "BEGINNING"

	}

	if e.AgentName != ""{

		subReconnect = true

	} 

	e.roundRobin = 0

	if e.Worker > 0{
		runtime.GOMAXPROCS(e.Worker)
	}

	e.autoIncr = 0

	var agentErr error

	e.AgentName, agentErr = os.Hostname()

	e.requestChan = make(chan bool, 1)

	if agentErr != nil{
		
		go log.Println(agentErr)

		return

	}

	if e.PoolSize > 0{

		var connectStatus = true

		for i := 0; i < e.PoolSize; i++ {

			e.Conn = e.createConnection()

			if e.Conn == nil{

				connectStatus = false

				break

			}

			e.ConnPool = append(e.ConnPool, e.Conn)	

		}

		if !connectStatus{

			time.Sleep(2 * time.Second)

			e.Connect()

		}

	}else{

		e.Conn = e.createConnection()

		if e.Conn == nil{

			time.Sleep(2 * time.Second)

			e.Connect()

			return

		}

		e.ConnPool = append(e.ConnPool, e.Conn)	

	}

	if len(e.requestPull) > 0{

		var chancb = make(chan bool, 1)

		for _, bodyMap := range e.requestPull{

			go e.Publish(bodyMap, chancb)

			<-chancb
		}

	}

	if e.AppType == "producer"{

		go log.Println("Application started as producer...")

		if e.Acknowledge{

			if e.PoolSize > 0{

				for index := range e.ConnPool{

					go e.ReceiveMsg(e.ConnPool[index])

				}

			}else{

				go e.ReceiveMsg(e.ConnPool[0])

			}

		}
	}

	if e.AppType == "consumer"{

		go log.Println("Application started as consumer...")

		if e.PoolSize > 0{

			for index := range e.ConnPool{

				go e.ReceiveSubMsg(e.ConnPool[index])

			}

		}else{

			go e.ReceiveSubMsg(e.ConnPool[0])

		}

		if subReconnect{

			for{

				time.Sleep(1 * time.Second)

				if e.Conn != nil{
					break
				}
			}

			e.Subscribe(e.contentMatcher)
		}
	} 

	e.connectStatus = true

	if e.AuthReconnect{

		go e.checkConnectStatus()
		
	}
}

func (e *CreateProperties) checkConnectStatus(){

	defer handlepanic()

	for{

		time.Sleep(2 * time.Second)

		if e.connectStatus == false{

			e.ConnPool = e.ConnPool[:0]

			e.subscribeFD.Close()

			e.Connect()

			break

		}
	}

}

func (e *CreateProperties) createConnection() net.Conn{

	defer handlepanic()

	var conn net.Conn

	var err error

	url, err := url.Parse(e.Url)

    if err != nil {
        go log.Println(err)
        return nil
    }

	host, port, _ := net.SplitHostPort(url.Host)

	dest := host + ":" + port

	if e.ConnectionType != "tcp" && e.ConnectionType != "udp"{

		conn, err = net.Dial("tcp", dest)

	}else if e.ConnectionType == "tcp"{

		conn, err = net.Dial("tcp", dest)

	}else if e.ConnectionType == "udp"{

		conn, err = net.Dial("udp", dest)

	}

	if err != nil{

		go log.Println(err)

		return nil
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

	return conn
}

func (e *CreateProperties) Publish(bodyBB []byte, channcb chan bool){

	defer handlepanic()

	if !e.connectStatus{

		e.requestPull = append(e.requestPull, bodyBB)

		channcb <- false

		return

	}

	if e.WriteDelay > 0{
		time.Sleep(time.Duration(e.WriteDelay) * time.Nanosecond)
	}
	

	if e.PoolSize > 0{

		if e.roundRobin == e.PoolSize{

			e.roundRobin = 0

		}

		if e.roundRobin >= e.PoolSize{

			channcb <- false

			return

		}
		
		go e.publishMsg(bodyBB, e.ConnPool[e.roundRobin])

		<-e.requestChan

		e.roundRobin += 1

	}else{

	 	go e.publishMsg(bodyBB, e.ConnPool[0])

	 	<-e.requestChan

	}

	channcb <- true
}

func (e *CreateProperties) publishMsg(bodyBB []byte, conn net.Conn){

	defer handlepanic()

	e.Lock()

	defer e.Unlock()

	if conn == nil{

		go log.Println("No connection is made...")

		e.requestChan <- false

		return

	}

	e.autoIncr += 1
	currentTime := time.Now()
	var nano = currentTime.UnixNano()
	var _id = strconv.FormatInt(nano, 10)
	var producer_id = _id+"_"+strconv.FormatInt(e.autoIncr, 10)

	var byteBuffer = ByteBuffer.Buffer{
		Endian:"big",
	}

	var messageType = "publish"

	var messageTypeLen = len(messageType)

	var channelNameLen = len(e.ChannelName)

	var producer_idLen = len(producer_id)

	var agentNameLen = len(e.AgentName)

	// totalLen + messageTypelen + messageType + channelNameLen + channelName + producerIdLen + producerID + agentNameLen + agentName + totalBytePacket
	var totalByteLen = 2 + messageTypeLen + 2 + channelNameLen + 2 + producer_idLen + 2 + agentNameLen + 1 + len(bodyBB)
	 
	byteBuffer.PutLong(totalByteLen)

	byteBuffer.PutShort(messageTypeLen)

	byteBuffer.Put([]byte(messageType))

	byteBuffer.PutShort(channelNameLen)

	byteBuffer.Put([]byte(e.ChannelName))

	byteBuffer.PutShort(producer_idLen)

	byteBuffer.Put([]byte(producer_id))

	byteBuffer.PutShort(agentNameLen)

	byteBuffer.Put([]byte(e.AgentName))

	if e.Acknowledge{

		byteBuffer.PutByte(byte(1))
		
	}else{

		byteBuffer.PutByte(byte(0))
	}

	byteBuffer.Put(bodyBB)

	var byteArrayResp = byteBuffer.Array()
 
	if e.Acknowledge{

		e.TransactionList[producer_id] = byteArrayResp	

	}

	_, err := conn.Write(byteArrayResp)

	if err != nil {

		e.connectStatus = false

		e.requestPull = append(e.requestPull, bodyBB)

		go log.Println(err)

		e.requestChan <- false

		return
	}

	e.requestChan <- true
}

func (e *CreateProperties) Close(){

	defer handlepanic()

	for index := range e.ConnPool{

		e.ConnPool[index].Close()
	}

	go log.Println("Socket closed...")

}

func (e *CreateProperties) Subscribe(contentMatcher string) bool{

	defer handlepanic()

	if contentMatcher != ""{

		e.contentMatcher = contentMatcher

		e.contentMatcherMap = make(map[string]interface{})

		errJson := json.Unmarshal([]byte(e.contentMatcher), &e.contentMatcherMap)

		if errJson != nil{
			
			go log.Println(errJson)
			
			return false

		}
	}

	// totalLen + messageTypelen + messageType + channelNameLen + channelName + startFromLen + startFrom + subscriberTypeLen + subscriberType

	// subscriberType = "Group | Individual"

	var messageType = "subscribe"

	var messageTypeLen = len(messageType)

	var channelNameLen = len(e.ChannelName)

	var startFromLen = len(e.AlwaysStartFrom)

	var SubscriberNameLen = len(e.SubscriberName)

	var subscriberTypeLen = len(e.GroupName)

	var byteBuffer = ByteBuffer.Buffer{
		Endian:"big",
	}

	var totalLen = 2 + messageTypeLen + 2 + channelNameLen + 2 + startFromLen + 2 + SubscriberNameLen + 2 + subscriberTypeLen

	byteBuffer.PutLong(totalLen) // 8

	byteBuffer.PutShort(messageTypeLen) // 2

	byteBuffer.Put([]byte(messageType)) // messageTypeLen

	byteBuffer.PutShort(channelNameLen) // 2

	byteBuffer.Put([]byte(e.ChannelName)) // channelNameLen

	byteBuffer.PutShort(startFromLen) // 2

	byteBuffer.Put([]byte(e.AlwaysStartFrom)) // startFromLen

	byteBuffer.PutShort(SubscriberNameLen) // 2

	byteBuffer.Put([]byte(e.SubscriberName)) // SubscriberNameLen

	byteBuffer.PutShort(subscriberTypeLen) // subscriberTypeLen

	byteBuffer.Put([]byte(e.GroupName))

	_, err := e.Conn.Write(byteBuffer.Array())

	if err != nil {

		e.connectStatus = false

		go log.Println(err)

		return false

	}

	return true
} 

func allZero(s []byte) bool {

	defer handlepanic()

	for _, v := range s {

		if v != 0 {

			return false

		}

	}

	return true
}

func (e *CreateProperties) ReceiveSubMsg(conn net.Conn){

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

	go log.Println("Socket disconnected...")

	conn.Close()

	e.connectStatus = false
}

func (e *CreateProperties) ReceiveMsg(conn net.Conn){

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

		completePacket := make([]byte, packetSize)

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

func (e *CreateProperties) parseMsg(packetSize int64, message []byte, msgType string, callbackChan chan string){

	defer handlepanic()

	if msgType == "pub"{

		var producer_id = string(message)

		callbackChan <- string(producer_id)

	}

	if msgType == "sub"{	

		var byteBuffer = ByteBuffer.Buffer{
			Endian:"big",
		}

		byteBuffer.Wrap(message)

		var messageTypeByte = byteBuffer.GetShort()
		var messageTypeLen = int(binary.BigEndian.Uint16(messageTypeByte))
		byteBuffer.Get(messageTypeLen)

		var channelNameByte = byteBuffer.GetShort()
		var channelNameLen = int(binary.BigEndian.Uint16(channelNameByte))
		byteBuffer.Get(channelNameLen)

		var producer_idByte = byteBuffer.GetShort()
		var producer_idLen = int(binary.BigEndian.Uint16(producer_idByte))
		byteBuffer.Get(producer_idLen)

		var agentNameByte = byteBuffer.GetShort()
		var agentNameLen = int(binary.BigEndian.Uint16(agentNameByte))
		byteBuffer.Get(agentNameLen)

		byteBuffer.GetLong() //id

		var bodyPacketSize = packetSize - int64(2 + messageTypeLen + 2 + channelNameLen + 2 + producer_idLen + 2 + agentNameLen + 8)

		var bodyPacket = byteBuffer.Get(int(bodyPacketSize))

		if len(e.contentMatcherMap) != 0{

			var messageData = make(map[string]interface{})

			errJson := json.Unmarshal(bodyPacket, &messageData)

			if errJson != nil{
				
				go log.Println(errJson)
					
				callbackChan <- "REJECT"

				return

			}

			e.contentMatch(messageData)

		}else{

			SubscriberChannel <- bodyPacket

		}

		callbackChan <- "SUCCESS"	
	}
}

func (e *CreateProperties) contentMatch(messageData map[string]interface{}){

	var matchFound = true

	if _, found := e.contentMatcherMap["$and"]; found {

	    matchFound = AndMatch(messageData, e.contentMatcherMap)

	}else if _, found := e.contentMatcherMap["$or"]; found {

		matchFound = OrMatch(messageData, e.contentMatcherMap)

	}else if _, found := e.contentMatcherMap["$eq"]; found {

		if e.contentMatcherMap["$eq"] == "all"{

			matchFound = true

		}else{

			matchFound = false

		}

	}else{

		matchFound = false

	}

	if matchFound{

		SubscriberChannel <- messageData

	}

}