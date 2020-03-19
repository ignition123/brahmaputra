package brahmaputra

import(
	"log"
	"net"
	"encoding/binary"
	"time"
	"strconv"
	"os"
	"bytes"
	"encoding/json"
	"sync"
	"runtime"
)

var SubscriberChannel = make(chan map[string]interface{})

type CreateProperties struct{
	Host string
	Port string
	AuthToken string
	ConnectionType string
	Conn net.Conn
	ChannelName string
	AgentName string
	TransactionList map[string]interface{}
	AppType string
	autoIncr int64
	Worker int

	sync.Mutex

	ConnPool []net.Conn

	PoolSize int

	roundRobin int

	requestPull []map[string]interface{}

	connectStatus bool

	requestWg sync.WaitGroup

	ReceiveSync sync.WaitGroup

	requestChan chan bool

	contentMatcher string
}	

func (e *CreateProperties) Connect(){

	if e.AppType != "producer" && e.AppType != "consumer"{

		go log.Println("AppType must be producer or consumer...")

		return

	}

	var subReconnect = false

	if e.AgentName != ""{

		subReconnect = true

	}

	e.roundRobin = 0

	if e.Worker > 0{
		runtime.GOMAXPROCS(e.Worker)
	}

	e.autoIncr = 0

	e.TransactionList = make(map[string]interface{})

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

			select {

				case _, ok := <-chancb :	

					if ok{

					}
				break
			}
		}

	}

	if e.AppType == "producer"{

		go log.Println("Application started as producer...")

		if e.PoolSize > 0{

			for index := range e.ConnPool{

				go e.ReceiveMsg(e.ConnPool[index])

			}

		}else{

			go e.ReceiveMsg(e.ConnPool[0])

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

	go e.checkConnectStatus()
}

func (e *CreateProperties) checkConnectStatus(){

	for{

		time.Sleep(2 * time.Second)

		if e.connectStatus == false{

			e.ConnPool = e.ConnPool[:0]

			e.Connect()

			break

		}
	}

}

func (e *CreateProperties) createConnection() net.Conn{

	var conn net.Conn

	var err error

	dest := e.Host + ":" + e.Port

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

func (e *CreateProperties) Publish(bodyBB map[string]interface{}, channcb chan bool){

	if !e.connectStatus{

		go log.Println("Connection lost...")

		e.requestPull = append(e.requestPull, bodyBB)

		channcb <- false

		return

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

		select {

			case _, ok := <-e.requestChan :	

				if ok{

				}
			break
		}

		e.roundRobin += 1

	}else{

	 	go e.publishMsg(bodyBB, e.ConnPool[0])

	 	select {

			case _, ok := <-e.requestChan :	

				if ok{

				}
			break
		}

	}

	channcb <- true
}

func (e *CreateProperties) publishMsg(bodyBB map[string]interface{}, conn net.Conn){

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

	var messageMap = make(map[string]interface{})

	messageMap["channelName"] = e.ChannelName

	messageMap["type"] = "publish"

	messageMap["producer_id"] = producer_id

	messageMap["AgentName"] = e.AgentName

	messageMap["data"] = bodyBB

	e.Lock()
	e.TransactionList[producer_id] = messageMap
	e.Unlock()

	var packetBuffer bytes.Buffer
 
	buff := make([]byte, 4)

	jsonData, err := json.Marshal(messageMap)

	if err != nil{

		go log.Println(err)

		e.requestChan <- false

		return

	}

	binary.LittleEndian.PutUint32(buff, uint32(len(jsonData)))

	packetBuffer.Write(buff)

	packetBuffer.Write(jsonData)

	_, err = conn.Write(packetBuffer.Bytes())

	messageMap = nil

	if err != nil {

		e.connectStatus = false

		e.requestPull = append(e.requestPull, bodyBB)

		go log.Println(err)

		e.requestChan <- false

		return
	}

	e.requestChan <- true
}

func (e *CreateProperties) Close(conn net.Conn){

	conn.Close()

	go log.Println("Socket closed...")

}

func (e *CreateProperties) Subscribe(contentMatcher string){

	e.contentMatcher = contentMatcher

	var jsonObject = make(map[string]interface{})

	jsonErr := json.Unmarshal([]byte(contentMatcher), &jsonObject)

	if jsonErr != nil{

		go log.Println(jsonErr)
		return

	}	

	var messageMap = make(map[string]interface{})
	messageMap["contentMatcher"] = jsonObject
	messageMap["channelName"] = e.ChannelName
	messageMap["type"] = "subscribe"

	jsonData, err := json.Marshal(messageMap)

	if err != nil{

		go log.Println(err)

		return

	}

	var packetBuffer bytes.Buffer

	buff := make([]byte, 4)

	binary.LittleEndian.PutUint32(buff, uint32(len(jsonData)))

	packetBuffer.Write(buff)

	packetBuffer.Write(jsonData)

	_, err = e.Conn.Write(packetBuffer.Bytes())

	messageMap = nil

	if err != nil {

		e.connectStatus = false

		go log.Println(err)

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

func (e *CreateProperties) ReceiveSubMsg(conn net.Conn){

	var callbackChan = make(chan string, 1)

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

			go log.Println("Socket disconnected...")

			break
		}

		go e.parseMsg(completePacket, "sub", callbackChan)

		select {

			case _, ok := <-callbackChan:	

				if ok{

				}
			break
		}
	}

	go log.Println("Socket disconnected...")

	conn.Close()

	e.connectStatus = false
}


func (e *CreateProperties) ReceiveMsg(conn net.Conn){

	var callbackChan = make(chan string, 1)

	for {	

		sizeBuf := make([]byte, 4)

		conn.Read(sizeBuf)

		packetSize := binary.LittleEndian.Uint32(sizeBuf)

		sizeBuf = nil

		if packetSize < 0 {
			continue
		}

		completePacket := make([]byte, packetSize)

		conn.Read(completePacket)

		if allZero(completePacket) {

			go log.Println("Socket disconnected...")

			break
		}

		go e.parseMsg(completePacket, "pub", callbackChan)

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

func (e *CreateProperties) parseMsg(message []byte, msgType string, callbackChan chan string){

	messageMap := make(map[string]interface{})

	err := json.Unmarshal(message, &messageMap)

	if err != nil{
		
		go log.Println(err)

		callbackChan <- "REJECT"

		return

	}

	if msgType == "pub"{

		var producer_id = messageMap["producer_id"].(string)

		callbackChan <- producer_id

	}

	if msgType == "sub"{

		SubscriberChannel <- messageMap

		callbackChan <- "SUCCESS"

	}

	messageMap = nil
}
