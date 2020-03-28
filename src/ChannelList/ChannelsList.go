package ChannelList

import(
	"pojo"
	"sync"
)

var ConfigTCPObj pojo.Config

var TCPStorage = make(map[string] *pojo.ChannelStruct)
var UDPStorage = make(map[string] *pojo.ChannelStruct)

var TCPSocketDetails = make(map[string] []*pojo.PacketStruct)
var UDPSocketDetails = make(map[string] []*pojo.SocketDetails)

type TCPGrp struct{
	sync.Mutex
	TCPSubGroup map[string] map[string][]*pojo.PacketStruct
	TCPChannelSubscriberList map[string]bool
}
