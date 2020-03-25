package ChannelList

import(
	"pojo"
)

var ConfigTCPObj pojo.Config

var TCPStorage = make(map[string] *pojo.ChannelStruct)
var UDPStorage = make(map[string] *pojo.ChannelStruct)
var TCPChannelSubscriberList = make(map[string]bool)
var TCPSubscriberGroup = make(map[string] map[string][]*pojo.PacketStruct)

var TCPSocketDetails = make(map[string] []*pojo.PacketStruct)
var UDPSocketDetails = make(map[string] []*pojo.SocketDetails)
