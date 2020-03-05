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
	TransactionList map[int64]interface{}
	sendMsg chan bool
	AppType string
	Persitence bool
	LogPath string
}	

var RequestMutex = &sync.Mutex{}

func (e *CreateProperties) Connect() net.Conn{

	if e.AppType != "producer" && e.AppType != "consumer"{

		fmt.Println("AppType must be producer or consumer...")

		return nil

	}

	e.TransactionList = make(map[int64]interface{})

	var agentErr error

	e.AgentName , agentErr = os.Hostname()

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

// var count = 0

func (e *CreateProperties) Publish(bodyBB map[string]interface{}){

	// count += 1

	RequestMutex.Lock()
	defer RequestMutex.Unlock()

	currentTime := time.Now()

	var nano = currentTime.UnixNano()

	var _id = strconv.FormatInt(nano, 10)

	var messageMap = make(map[string]interface{})

	messageMap["channelName"] = e.ChannelName

	messageMap["type"] = "publish"

	messageMap["producer_id"] = _id

	messageMap["AgentName"] = e.AgentName

	messageMap["data"] = bodyBB

	e.TransactionList[nano] = messageMap

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

	// fmt.Println(count)

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

	messageMap := make(map[string]interface{})

	err := json.Unmarshal(message, &messageMap)

	if err != nil{
		
		fmt.Println(err)

		return

	}

	var producer_id = messageMap["producer_id"].(string)

	nanoId, err := strconv.ParseInt(producer_id, 10, 64)

	if err == nil {

	    fmt.Println(err)

	    return

	}

	delete(e.TransactionList, nanoId)

	fmt.Println(e.TransactionList)
}