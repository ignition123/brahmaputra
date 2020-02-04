package server

import(
	"pojo"
)

var TCPStorage = make(map[string] *pojo.ChannelStruct)
var UDPStorage = make(map[string] *pojo.ChannelStruct)
