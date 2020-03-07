package brahmaputra

import(
	"fmt"
	"net"
	"encoding/binary"
	"time"
	"strconv"
	"os"
	"bytes"
	"encoding/json"
	"sync"
	_"context"
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
	sendMsg chan bool
	AppType string
	Persitence bool
	LogPath string
	autoIncr int64
	SubscribeMsg chan map[string]interface{}
}	

var RequestMutex = &sync.Mutex{}

func (e *CreateProperties) Connect() net.Conn{

	if e.AppType != "producer" && e.AppType != "consumer"{

		fmt.Println("AppType must be producer or consumer...")

		return nil

	}

	e.autoIncr = 0

	e.TransactionList = make(map[string]interface{})

	var agentErr error

	e.AgentName, agentErr = os.Hostname()

	if agentErr != nil{
		
		fmt.Println(agentErr)

		return nil

	}

	dest := e.Host + ":" + e.Port

	var err error

	if e.ConnectionType != "tcp" && e.ConnectionType != "udp"{

		e.Conn, err = net.Dial("tcp", dest)

	}else if e.ConnectionType == "tcp"{

		e.Conn, err = net.Dial("tcp", dest)

	}else if e.ConnectionType == "udp"{

		e.Conn, err = net.Dial("udp", dest)

	}

	if err != nil{

		fmt.Println(err)

		return nil
	}

	e.Conn.(*net.TCPConn).SetKeepAlive(true)
	e.Conn.(*net.TCPConn).SetKeepAlive(true)
	e.Conn.(*net.TCPConn).SetLinger(1)
	e.Conn.(*net.TCPConn).SetNoDelay(true)
	e.Conn.(*net.TCPConn).SetReadBuffer(10000)
	e.Conn.(*net.TCPConn).SetWriteBuffer(10000)
	e.Conn.(*net.TCPConn).SetDeadline(time.Now().Add(1000000 * time.Second))
	e.Conn.(*net.TCPConn).SetReadDeadline(time.Now().Add(1000000 * time.Second))
	e.Conn.(*net.TCPConn).SetWriteDeadline(time.Now().Add(1000000 * time.Second))

	if e.AppType == "producer"{

		fmt.Println("Application started as producer...")

		go e.ReceiveMsg()

	}

	if e.AppType == "consumer"{

		fmt.Println("Application started as consumer...")

		go e.ReceiveSubMsg()

	} 
	

	return e.Conn
}

func (e *CreateProperties) Publish(bodyBB map[string]interface{}){

	if e.Conn == nil{

		fmt.Println("No connection is made...")

		return

	}

	RequestMutex.Lock()
	defer RequestMutex.Unlock()

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

	var packetBuffer bytes.Buffer
 
	buff := make([]byte, 4)

	jsonData, err := json.Marshal(messageMap)

	if err != nil{

		fmt.Println(err)

		return

	}

	binary.LittleEndian.PutUint32(buff, uint32(len(jsonData)))

	packetBuffer.Write(buff)

	packetBuffer.Write(jsonData)

	// fmt.Println(string(jsonData))

	_, err = e.Conn.Write(packetBuffer.Bytes())

	messageMap = nil

	if err != nil {

		fmt.Println(err)

	}
}

func (e *CreateProperties) Close(){

	e.Conn.Close()
	fmt.Println("Socket closed...")

}

func (e *CreateProperties) Subscribe(contentMatcher string){

	var jsonObject = make(map[string]interface{})

	jsonErr := json.Unmarshal([]byte(contentMatcher), &jsonObject)

	if jsonErr != nil{

		fmt.Println(jsonErr)
		return

	}	

	var messageMap = make(map[string]interface{})
	messageMap["contentMatcher"] = jsonObject
	messageMap["channelName"] = e.ChannelName
	messageMap["type"] = "subscribe"

	jsonData, err := json.Marshal(messageMap)

	if err != nil{

		fmt.Println(err)

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
		fmt.Println(err)
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

func (e *CreateProperties) ReceiveSubMsg(){

	for {	

		sizeBuf := make([]byte, 4)

		e.Conn.Read(sizeBuf)

		packetSize := binary.LittleEndian.Uint32(sizeBuf)

		if packetSize < 0 {
			continue
		}

		sizeBuf = nil

		completePacket := make([]byte, packetSize)

		e.Conn.Read(completePacket)

		if allZero(completePacket) {

			fmt.Println("Socket disconnected...")

			break
		}

		e.parseMsg(completePacket, "sub")
	}

	fmt.Println("Socket disconnected...")

	e.Conn.Close()

}

func (e *CreateProperties) ReceiveMsg(){

	for {	

		sizeBuf := make([]byte, 4)

		e.Conn.Read(sizeBuf)

		packetSize := binary.LittleEndian.Uint32(sizeBuf)

		sizeBuf = nil

		if packetSize < 0 {
			continue
		}

		completePacket := make([]byte, packetSize)

		e.Conn.Read(completePacket)

		if allZero(completePacket) {

			fmt.Println("Socket disconnected...")

			break
		}

		e.parseMsg(completePacket, "pub")
	}

	fmt.Println("Socket disconnected...")

	e.Conn.Close()
}

func (e *CreateProperties) parseMsg(message []byte, msgType string){

	RequestMutex.Lock()
	defer RequestMutex.Unlock()

	messageMap := make(map[string]interface{})

	err := json.Unmarshal(message, &messageMap)

	if err != nil{
		
		fmt.Println(err)

		return

	}

	if msgType == "pub"{

		var producer_id = messageMap["producer_id"].(string)

		delete(e.TransactionList, producer_id)
	}

	if msgType == "sub"{

		e.SubscribeMsg <- messageMap

	}

	messageMap = nil
}

func (e *CreateProperties) GetSubscribeMessage(chann chan map[string]interface{}){

	e.SubscribeMsg = chann
}