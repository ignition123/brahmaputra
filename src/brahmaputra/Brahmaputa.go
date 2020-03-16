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
	_"context"
	"runtime"
)

	// ctx, _ := context.WithTimeout(context.Background(), 15 * time.Second)

type CreateProperties struct{
	Host string
	Port string
	AuthToken string
	ConnectionType string
	AutoReconnect bool
	RetryPeriod int // in minutes
	Conn net.Conn
	ChannelName string
	AgentName string
	TransactionList map[string]interface{}
	AppType string
	Persitence bool
	LogPath string
	autoIncr int64
	SubscribeMsg chan map[string]interface{}
	Worker int

	sync.Mutex

	ConnPool []net.Conn

	PoolSize int

	roundRobin int
}	

func (e *CreateProperties) Connect(){

	if e.AppType != "producer" && e.AppType != "consumer"{

		log.Println("AppType must be producer or consumer...")

		return

	}

	e.roundRobin = 0

	e.SubscribeMsg = make(chan map[string]interface{}, 1)

	if e.Worker > 0{
		runtime.GOMAXPROCS(e.Worker)
	}

	e.autoIncr = 0

	e.TransactionList = make(map[string]interface{})

	var agentErr error

	e.AgentName, agentErr = os.Hostname()

	if agentErr != nil{
		
		log.Println(agentErr)

		return

	}

	if e.PoolSize > 0{

		for i := 0; i < e.PoolSize; i++ {

			e.Conn = e.createConnection()

			e.ConnPool = append(e.ConnPool, e.Conn)	

		}

	}else{

		e.Conn = e.createConnection()

		e.ConnPool = append(e.ConnPool, e.Conn)	

	}

	

	if e.AppType == "producer"{

		log.Println("Application started as producer...")

		if e.PoolSize > 0{

			for index := range e.ConnPool{

				go e.ReceiveMsg(e.ConnPool[index])

			}

		}else{

			go e.ReceiveMsg(e.ConnPool[0])

		}
	}

	if e.AppType == "consumer"{

		log.Println("Application started as consumer...")

		if e.PoolSize > 0{

			for index := range e.ConnPool{

				go e.ReceiveSubMsg(e.ConnPool[index])

			}

		}else{

			go e.ReceiveSubMsg(e.ConnPool[0])

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

		log.Println(err)

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

func (e *CreateProperties) Publish(bodyBB map[string]interface{}, wg *sync.WaitGroup){

	defer wg.Done()

	if e.PoolSize > 0{

		e.Lock()

		e.roundRobin += 1

		if e.roundRobin == e.PoolSize{

			e.roundRobin = 0

		}

		e.Unlock()

		go e.publishMsg(bodyBB, e.ConnPool[e.roundRobin])

	}else{

		go e.publishMsg(bodyBB, e.ConnPool[0])

	}
}

func (e *CreateProperties) publishMsg(bodyBB map[string]interface{}, conn net.Conn){

	if conn == nil{

		log.Println("No connection is made...")

		return

	}

	e.Lock()

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

	e.TransactionList[producer_id] = messageMap

	e.Unlock()
	
	var packetBuffer bytes.Buffer
 
	buff := make([]byte, 4)

	jsonData, err := json.Marshal(messageMap)

	if err != nil{

		log.Println(err)

		return

	}

	binary.LittleEndian.PutUint32(buff, uint32(len(jsonData)))

	packetBuffer.Write(buff)

	packetBuffer.Write(jsonData)

	e.Lock()

	_, err = conn.Write(packetBuffer.Bytes())

	e.Unlock()

	messageMap = nil

	if err != nil {

		log.Println(err)

	}

}

func (e *CreateProperties) Close(conn net.Conn){

	conn.Close()

	log.Println("Socket closed...")

}

func (e *CreateProperties) Subscribe(contentMatcher string){

	var jsonObject = make(map[string]interface{})

	jsonErr := json.Unmarshal([]byte(contentMatcher), &jsonObject)

	if jsonErr != nil{

		log.Println(jsonErr)
		return

	}	

	var messageMap = make(map[string]interface{})
	messageMap["contentMatcher"] = jsonObject
	messageMap["channelName"] = e.ChannelName
	messageMap["type"] = "subscribe"

	jsonData, err := json.Marshal(messageMap)

	if err != nil{

		log.Println(err)

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
		log.Println(err)
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

	for {	

		sizeBuf := make([]byte, 4)

		conn.Read(sizeBuf)

		packetSize := binary.LittleEndian.Uint32(sizeBuf)

		if packetSize < 0 {
			continue
		}

		sizeBuf = nil

		completePacket := make([]byte, packetSize)

		conn.Read(completePacket)

		if allZero(completePacket) {

			log.Println("Socket disconnected...")

			break
		}

		go e.parseMsg(completePacket, "sub")
	}

	log.Println("Socket disconnected...")

	conn.Close()

}

func (e *CreateProperties) ReceiveMsg(conn net.Conn){

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

			log.Println("Socket disconnected...")

			break
		}

		go e.parseMsg(completePacket, "pub")
	}

	log.Println("Socket disconnected...")

	conn.Close()
}

func (e *CreateProperties) parseMsg(message []byte, msgType string){

	messageMap := make(map[string]interface{})

	err := json.Unmarshal(message, &messageMap)

	if err != nil{
		
		log.Println(err)

		return

	}

	if msgType == "pub"{

		var producer_id = messageMap["producer_id"].(string)

		e.Lock()

		delete(e.TransactionList, producer_id)

		e.Unlock()
	}

	if msgType == "sub"{

		e.SubscribeMsg <- messageMap

	}

	messageMap = nil
}

func (e *CreateProperties) GetSubscribeMessage(chann chan map[string]interface{}){

	e.SubscribeMsg = chann
}