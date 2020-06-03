package ChannelList

import(
	"pojo"
)

// global object to store channel config inmemory and packet object + producer and subscriber details

var ConfigTCPObj pojo.Config
var ConfigUDPObj pojo.Config

var TCPStorage = make(map[string] *pojo.ChannelStruct)
var TCPSocketDetails = make(map[string] map[string]*pojo.PacketStruct)


