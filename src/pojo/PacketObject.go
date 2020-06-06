package pojo

import(
	"net"
	"os"
)

// client objetc

type ClientObject struct{
	CounterRequest int
	SubscriberMapName string
	ChannelMapName string
	MessageMapType string
	GroupMapName string
}

// packet struct, packet received fromt the client

type PacketStruct struct{
	MessageTypeLen int
	MessageType string
	ChannelNameLen int
	ChannelName string
	Conn net.TCPConn
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
	SubscriberFD []*os.File 
	ProducerAck bool
	CompressionType byte
	ActiveMode bool
}
