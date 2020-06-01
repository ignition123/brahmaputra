package brahmaputra

import(
	"net"
	"log"
	"time"
	"strconv"
	"ByteBuffer"
	"encoding/json"
)

func allZero(s []byte) bool {

	defer handlepanic()

	for _, v := range s {

		if v != 0 {

			return false

		}

	}

	return true
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
	var totalByteLen = 2 + messageTypeLen + 2 + channelNameLen  + 2 + producer_idLen + 2 + agentNameLen + 1 + len(bodyBB)     
	 
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

		byteBuffer.PutByte(byte(2))
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