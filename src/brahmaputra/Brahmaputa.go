package brahmaputra

/*
	client driver main file
*/

// importing modules

import(
	"log"
	"net"
	"os"
	"sync"
	"runtime"
)

// creating susbcriber channel with type interface

var SubscriberChannel = make(chan interface{}, 1)

// TCP Config structure

type TCPConf struct{
	SocketReadTimeout int
	SocketWriteTimeout int
	BufferRead int
	Timeout int
	Linger int
	KeepAlive bool
	NoDelay bool
}

// creating a structure with many fields as configuration

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
	subContentmatcher bool
	Compression string
	TCP *TCPConf
}	

// compression constant

const(
	noCompression = 1
	zlibCompression = 2
	gzipCompression = 3
	snappyCompression = 4
	lzCompression = 5
)

// mathod to handle panics

func handlepanic() { 
  
    if a := recover(); a != nil { 
        log.Println(a)
    } 
} 

// validating the connection type and app type

func (e *CreateProperties) validateFields() bool {

	if e.ConnectionType != "tcp" && e.ConnectionType != "ws"{

		go log.Println("ConnectionType must be tcp or ws...")

		return false

	}

	if e.AppType != "producer" && e.AppType != "consumer"{

		go log.Println("AppType must be producer or consumer...")

		return false

	}

	return true
}

// method to connect tcp server

func (e *CreateProperties) Connect(){

	defer handlepanic()

	if !e.validateFields(){
		return
	}

	// by default reconnect flag is false

	e.subContentmatcher = false

	// checking the agent name

	if e.AgentName != ""{

		e.subContentmatcher = true

	} 

	// getting the agent name from the current login session

	var agentErr error

	e.AgentName, agentErr = os.Hostname()

	// creating a request boolean channel

	e.requestChan = make(chan bool, 1)

	if agentErr != nil{
		
		go log.Println(agentErr)

		return

	}

	if e.AppType == "producer"{

		// checking if producer acknowledgement is true

		if e.Acknowledge{

			e.TransactionList = make(map[string][]byte)
			
		}

		// setting round robin flag = 0 by default

		e.roundRobin = 0

		if e.Worker > 0{
			runtime.GOMAXPROCS(e.Worker)
		}

		// checking the compression set for the packet

		if e.Compression != ""{

			if e.Compression != "zlib" && e.Compression != "gzip" && e.Compression != "lz4" && e.Compression != "snappy"{

				go log.Println("Invalid compression, must be zlib, gzip, snappy or lz4 ...")

				return
			} 

		}

		// setting the autoincrement producer id = 0
 
		e.autoIncr = 0

	}else{

		// content matcher map hashmap

		e.contentMatcherMap = make(map[string]interface{})

		// checking always start from flags 

		if e.AlwaysStartFrom == ""{

			e.AlwaysStartFrom = "BEGINNING"

		}

	}

	if e.ConnectionType == "tcp"{

		e.connectTCP()

	}

}

// closing the tcp connection

func (e *CreateProperties) Close(){

	defer handlepanic()

	for index := range e.ConnPool{

		e.ConnPool[index].Close()
	}

	go log.Println("Socket closed...")

}
