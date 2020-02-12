package ChannelList

import(
	"pojo"
)

func IsClosed(ch <-chan string) bool {
	select {
	case <-ch:
		return true
	default:
	}

	return false
}

func IsMapClosed(ch <-chan map[string]interface{}) bool {
	select {
	case <-ch:
		return true
	default:
	}

	return false
}

func IsBoolClosed(ch <-chan bool) bool {
	select {
	case <-ch:
		return true
	default:
	}

	return false
}

var ConfigTCPObj pojo.Config

var TCPStorage = make(map[string] *pojo.ChannelStruct)
var UDPStorage = make(map[string] *pojo.ChannelStruct)

var TCPSocketDetails = make(map[string] []*pojo.SocketDetails)
var UDPSocketDetails = make(map[string] []*pojo.SocketDetails)