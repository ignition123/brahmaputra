package server

import (
	"pojo"
	"net"
	"fmt"
	_"os"
	"time"
)

type ServerUDPConnection struct{
	connections map[net.Conn] time.Time
}

func HostUDP(configObj pojo.Config){

	// LoadUDPChannelsToMemory()

	if *configObj.Server.UDP.Host != "" && *configObj.Server.UDP.Port != ""{
		HostUDPServer(configObj)
	}
}


func HostUDPServer(configObj pojo.Config){
	fmt.Println("Loading log files...")
	fmt.Println("Starting UDP server...")
}