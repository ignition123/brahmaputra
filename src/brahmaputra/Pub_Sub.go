package brahmaputra

/*
	publishing message to the server
*/

// importing the modules

import(
	"net"
	"log"
	"time"
	"strconv"
	"ByteBuffer"
	"encoding/json"
)

// publishing message to the server

func (e *CreateProperties) publishMsg(bodyBB []byte, conn net.Conn){

	defer handlepanic()

	// locking the method with mutex

	e.Lock()
	defer e.Unlock()

	// checking the tcp connection if it not nil

	if conn == nil{

		go log.Println("No connection is made...")

		e.requestChan <- false

		return

	}

	// auto incrementing the producerId
	e.autoIncr += 1

	// getting the current time
	currentTime := time.Now()

	// getting the current time in nano second
	nano := currentTime.UnixNano()

	// converting the nano long value to string
	_id := strconv.FormatInt(nano, 10)

	// appending the _id + _ + autoincrement id
	producer_id := _id+"_"+strconv.FormatInt(e.autoIncr, 10)

	// creating the byte buffer with big endian

	byteBuffer := ByteBuffer.Buffer{
		Endian:"big",
	}

	// setting the message type to publish

	messageType := "publish"

	// message type length

	messageTypeLen := len(messageType)

	// channel name length

	channelNameLen := len(e.ChannelName)

	// producer id length

	producer_idLen := len(producer_id)

	// agent name length

	agentNameLen := len(e.AgentName)

	// total length of the packet size

	// totalLen + messageTypelen + messageType + channelNameLen + channelName + producerIdLen + producerID + agentNameLen + agentName + totalBytePacket
	totalByteLen := 2 + messageTypeLen + 2 + channelNameLen  + 2 + producer_idLen + 2 + agentNameLen + 1 + len(bodyBB)  

	// pushing total body length   
	 
	byteBuffer.PutLong(totalByteLen)

	// pushing message type length

	byteBuffer.PutShort(messageTypeLen)

	// pushing message type

	byteBuffer.Put([]byte(messageType))

	// pushing channel name length

	byteBuffer.PutShort(channelNameLen)

	// pushing channel name

	byteBuffer.Put([]byte(e.ChannelName))

	// pushing producer id length

	byteBuffer.PutShort(producer_idLen)

	// pushing producer id

	byteBuffer.Put([]byte(producer_id))

	// pushing agent name length

	byteBuffer.PutShort(agentNameLen)

	// pushing agent name

	byteBuffer.Put([]byte(e.AgentName))

	// pushing producer acknowledgment flag

	if e.Acknowledge{

		byteBuffer.PutByte(byte(1))
		
	}else{

		byteBuffer.PutByte(byte(2))
	}

	// pushing actual body

	byteBuffer.Put(bodyBB)

	// converting the byte buffer to byte array

	byteArrayResp := byteBuffer.Array()
 
	if e.Acknowledge{

		e.TransactionList[producer_id] = byteArrayResp	

	}

	// writing to the tcp sockets
	
	_, err := conn.Write(byteArrayResp)

	if err != nil {

		e.connectStatus = false

		// appending the request for retry

		e.requestPull = append(e.requestPull, bodyBB)

		go log.Println(err)

		e.requestChan <- false

		return
	}

	e.requestChan <- true
}

// method to subscribe for messages

func (e *CreateProperties) Subscribe(contentMatcher string) bool{

	defer handlepanic()

	// if content matcher is not equals to empty

	if contentMatcher != ""{

		// setting the content matcher to the object

		e.contentMatcher = contentMatcher

		// unmarshaling the json to object

		e.contentMatcherMap = make(map[string]interface{})

		// unmarshalling the content macther

		errJson := json.Unmarshal([]byte(e.contentMatcher), &e.contentMatcherMap)

		if errJson != nil{
			
			go log.Println(errJson)
			
			return false

		}
	}

	// totalLen + messageTypelen + messageType + channelNameLen + channelName + startFromLen + startFrom + subscriberTypeLen + subscriberType

	// subscriberType = "Group | Individual"

	// adding message type as subscriber

	messageType := "subscribe"

	// getting the message type length

	messageTypeLen := len(messageType)

	// getting channel name length

	channelNameLen := len(e.ChannelName)

	// getting startfrom length

	startFromLen := len(e.AlwaysStartFrom)

	// getting the subscriber name length

	SubscriberNameLen := len(e.SubscriberName)

	// getting the subscriber type length

	subscriberTypeLen := len(e.GroupName)

	// creating byte buffer in big endian

	byteBuffer := ByteBuffer.Buffer{
		Endian:"big",
	}

	// getting the total length of the packet

	totalLen := 2 + messageTypeLen + 2 + channelNameLen + 2 + startFromLen + 2 + SubscriberNameLen + 2 + subscriberTypeLen

	// pushing total length

	byteBuffer.PutLong(totalLen) // 8

	// pushing message type length

	byteBuffer.PutShort(messageTypeLen) // 2

	// pushing message type

	byteBuffer.Put([]byte(messageType)) // messageTypeLen

	// pushing the channel name length

	byteBuffer.PutShort(channelNameLen) // 2

	// pushing the channel name

	byteBuffer.Put([]byte(e.ChannelName)) // channelNameLen

	// pushing the start from length

	byteBuffer.PutShort(startFromLen) // 2

	// pushing the start from variable

	byteBuffer.Put([]byte(e.AlwaysStartFrom)) // startFromLen

	// pushing the subscriber name length

	byteBuffer.PutShort(SubscriberNameLen) // 2

	// pushing the subscriber name

	byteBuffer.Put([]byte(e.SubscriberName)) // SubscriberNameLen

	// pushing the subscriber type length

	byteBuffer.PutShort(subscriberTypeLen) // subscriberTypeLen

	// pushing the group name

	byteBuffer.Put([]byte(e.GroupName))

	// writing to the tcp packet

	_, err := e.Conn.Write(byteBuffer.Array())

	if err != nil {

		e.connectStatus = false

		go log.Println(err)

		return false

	}

	return true
} 

// method to publish the message to the server

func (e *CreateProperties) Publish(bodyBB []byte, channcb chan bool){

	defer handlepanic()

	// checking connection status

	if !e.connectStatus{

		// appending the packet 

		e.requestPull = append(e.requestPull, bodyBB)

		channcb <- false

		return

	}

	// checking the write delay

	if e.WriteDelay > 0{
		time.Sleep(time.Duration(e.WriteDelay) * time.Nanosecond)
	}
	
	// checking if pool size is set

	if e.PoolSize > 0{

		if e.roundRobin == e.PoolSize{

			e.roundRobin = 0

		}

		// using round robin algorithm

		if e.roundRobin >= e.PoolSize{

			channcb <- false

			return

		}

		// publising to server
		
		go e.publishMsg(bodyBB, e.ConnPool[e.roundRobin])

		<-e.requestChan

		e.roundRobin += 1

	}else{

		// publishing in single tcp connection no pool size set

	 	go e.publishMsg(bodyBB, e.ConnPool[0])

	 	<-e.requestChan

	}

	channcb <- true
}