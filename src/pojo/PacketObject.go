package pojo

import(
	"net"
)

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
	SubscriberOffset int64
	StartFromLen int
	Start_from string

}