package ChannelList

import(
	"pojo"
)

var ConfigTCPObj pojo.Config
var ConfigUDPObj pojo.Config

var TCPStorage = make(map[string] *pojo.ChannelStruct)
var TCPSocketDetails = make(map[string] map[string]*pojo.PacketStruct)


