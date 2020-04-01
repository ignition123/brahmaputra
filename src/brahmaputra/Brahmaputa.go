package brahmaputra

import(
	"log"
	"net"
	"os"
	"sync"
	"runtime"
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
	subReconnect bool
}	

func handlepanic() { 
  
    if a := recover(); a != nil { 
        log.Println(a)
    } 
} 

func (e *CreateProperties) validateFields() bool {

	if e.ConnectionType != "tcp" && e.ConnectionType != "udp" && e.ConnectionType != "ws" && e.ConnectionType != "rtmp"{

		go log.Println("ConnectionType must be tcp, udp, ws or rtmp...")

		return false

	}

	if e.AppType != "producer" && e.AppType != "consumer"{

		go log.Println("AppType must be producer or consumer...")

		return false

	}

	return true
}

func (e *CreateProperties) Connect(){

	defer handlepanic()

	if !e.validateFields(){
		return
	}

	e.subReconnect = false

	if e.ConnectionType == "udp" {

		e.Acknowledge = false

	}

	if e.Acknowledge{

		e.TransactionList = make(map[string][]byte)
		
	}

	e.contentMatcherMap = make(map[string]interface{})

	if e.AlwaysStartFrom == ""{

		e.AlwaysStartFrom = "BEGINNING"

	}

	if e.AgentName != ""{

		e.subReconnect = true

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

	if e.ConnectionType == "tcp"{

		e.connectTCP()

	}else if e.ConnectionType == "udp"{

		e.connectUDP()

	}

}

func (e *CreateProperties) Close(){

	defer handlepanic()

	for index := range e.ConnPool{

		e.ConnPool[index].Close()
	}

	go log.Println("Socket closed...")

}
