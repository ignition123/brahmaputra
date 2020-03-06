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
)

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

	if e.AppType == "producer"{

		fmt.Println("Application started as producer...")

		go e.ReceiveMsg()

	}

	if e.AppType == "consumer"{

		fmt.Println("Application started as consumer...")

		go e.Subscribe()

	} 
	

	return e.Conn
}

func (e *CreateProperties) Publish(bodyBB map[string]interface{}){

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

	if err != nil {

		fmt.Println(err)

	}
}

func (e *CreateProperties) Close(){

	e.Conn.Close()
	fmt.Println("Socket closed...")

}

func (e *CreateProperties) Subscribe(){



} 

func allZero(s []byte) bool {

	for _, v := range s {

		if v != 0 {

			return false

		}

	}

	return true
}

func (e *CreateProperties) ReceiveMsg(){

	for {	

		sizeBuf := make([]byte, 4)

		e.Conn.Read(sizeBuf)

		packetSize := binary.LittleEndian.Uint32(sizeBuf)

		if packetSize < 0 {
			continue
		}

		completePacket := make([]byte, packetSize)

		e.Conn.Read(completePacket)

		if allZero(completePacket) {

			fmt.Println("Socket disconnected...")

			break
		}

		go e.parseMsg(completePacket)
	}

	fmt.Println("Socket disconnected...")

	e.Conn.Close()
}

func (e *CreateProperties) parseMsg(message []byte){

	RequestMutex.Lock()
	defer RequestMutex.Unlock()

	messageMap := make(map[string]interface{})

	err := json.Unmarshal(message, &messageMap)

	if err != nil{
		
		fmt.Println(err)

		return

	}

	var producer_id = messageMap["producer_id"].(string)

	delete(e.TransactionList, producer_id)
}