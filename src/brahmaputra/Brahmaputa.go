package brahmaputra

import(
	"fmt"
	"net"
	"encoding/binary"
	"time"
	"strconv"
	"os"
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
}	

func (e CreateProperties) Connect() net.Conn{

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

	fmt.Println(e.Host)
	fmt.Println(e.Port)
	fmt.Println(e.AuthToken)

	defer e.Conn.Close()

	go e.ReceiveMsg()

	return e.Conn


// publish code
// this.realmProps = FTL.createProperties();
//            this.realmProps.set(Realm.PROPERTY_STRING_USERNAME, TCP_CONFIG.FTL_USER_NAME);
//            this.realmProps.set(Realm.PROPERTY_STRING_USERPASSWORD, TCP_CONFIG.FTL_PASSWORD);
//            this.realmProps.set(Realm.PROPERTY_STRING_CLIENT_LABEL, TCP_CONFIG.FTL_LABEL);
//            this.realm = FTL.connectToRealmServer(TCP_CONFIG.FTL_URL,TCP_CONFIG.PUB_APP_NAME, this.realmProps);
//            this.pub = this.realm.createPublisher(TCP_CONFIG.PUB_Endpoint_Name);
//            this.msg = this.realm.createMessage(null);	


// subscribe code
// this.props = FTL.createProperties();
//            this.props.set(Realm.PROPERTY_STRING_USERNAME, TCP_CONFIG.FTL_USER_NAME);
//            this.props.set(Realm.PROPERTY_STRING_USERPASSWORD, TCP_CONFIG.FTL_PASSWORD);
//            this.realm = FTL.connectToRealmServer(TCP_CONFIG.FTL_URL, TCP_CONFIG.SUB_APP_NAME , this.props);
//            this.cm = this.realm.createContentMatcher("{\"Exchange\":\"NSE\",\"ExchangeSegment\":\"CM\"}");
//            this.sub = this.realm.createSubscriber(TCP_CONFIG.SUB_Endpoint_Name, this.cm, null);
//            this.queue = this.realm.createEventQueue();
//            this.queue.addSubscriber(this.sub, this);
	
}

func (e CreateProperties) Publish(){

	currentTime := time.Now()

	var _id = strconv.FormatInt(currentTime.UnixNano(), 10)

	var messageMap = make(map[string]interface{})

	messageMap["channelName"] = e.ChannelName

	messageMap["type"] = "publish"

	messageMap["producer_id"] = _id

	messageMap["AgentName"] = e.AgentName

	e.TransactionList[_id] = currentTime

}

func (e CreateProperties) Close(){

	e.Conn.Close()
	fmt.Println("Socket closed...")

}

func (e CreateProperties) Subscribe(){

	

} 

func allZero(s []byte) bool {

	for _, v := range s {

		if v != 0 {

			return false

		}

	}

	return true
}

func (e CreateProperties) ReceiveMsg(){

	for {

		sizeBuf := make([]byte, 4)

		e.Conn.Read(sizeBuf)

		packetSize := binary.LittleEndian.Uint32(sizeBuf)

		if packetSize < 0 {
			continue
		}

		completePacket := make([]byte, packetSize)

		e.Conn.Read(completePacket)

		if allZero(completePacket){

			fmt.Println("Socket disconnected...")

			break

		}

		var message = string(completePacket)

		fmt.Println(message)

	}

	e.Conn.Close()

}