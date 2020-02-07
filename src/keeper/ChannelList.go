package keeper

import(
	"pojo"
)

var TCPStorage = make(map[string] *pojo.ChannelStruct)
var UDPStorage = make(map[string] *pojo.ChannelStruct)

var TCPSocketDetails = make(map[string] []*pojo.SocketDetails)
var UDPSocketDetails = make(map[string] []*pojo.SocketDetails)

// var TCPConnTable = make([]map[string] )