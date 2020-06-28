package pojo

import(
	"net"
	"os"
	"time"
)

var SubscriberObj = make(map[string]*Subscribers)


// client objetc

type ClientObject struct{
	SubscriberMapName string
	ChannelMapName string
	MessageMapType string
	GroupMapName string
	Conn net.TCPConn
	Channel chan *PacketStruct
}

// packet struct, packet received fromt the client

type PacketStruct struct{
	MessageTypeLen int
	MessageType string
	ChannelNameLen int
	ChannelName string
	Producer_idLen int
	Producer_id string
	AgentNameLen int
	AgentName string
	BodyBB []byte
	Id int64
	StartFromLen int
	Start_from string
	SubscriberNameLen int
	SubscriberName string
	SubscriberTypeLen int
	GroupName string 
	ProducerAck bool
	CompressionType byte
	ActiveMode bool
	SubscriberFD []*os.File
}


type Subscribers struct{
	Channel *ChannelStruct
	Register  chan *ClientObject
	UnRegister chan *ClientObject
	GroupUnRegister chan string
	BroadCast chan *PacketStruct
	Clients map[*ClientObject] bool 
	Groups map[string] []*ClientObject 
	CurrentTime time.Time
}
