package server

import (
	"pojo"
	"net"
	"fmt"
	"os"
	"time"
)

type ServerTCPConnection struct{
	connections map[net.Conn] time.Time
}

var ConfigTCPObj pojo.Config

func HostTCP(configObj pojo.Config){

	ConfigTCPObj = configObj

	LoadTCPChannelsToMemory(ConfigTCPObj)

	GetChannelData()

	if *ConfigTCPObj.Server.TCP.Host != "" && *ConfigTCPObj.Server.TCP.Port != ""{
		HostTCPServer(ConfigTCPObj)
	}
}

func HostTCPServer(ConfigTCPObj pojo.Config){

	server, err := net.Listen("tcp", *ConfigTCPObj.Server.TCP.Host +":"+ *ConfigTCPObj.Server.TCP.Port)

    if err != nil {
        WriteLog("Error listening: "+err.Error())
        os.Exit(1)
	}
	
	defer server.Close()

	fmt.Println("Listening on " + *ConfigTCPObj.Server.TCP.Host + ":" + *ConfigTCPObj.Server.TCP.Port+"...")

	WriteLog("Loading log files...")
	WriteLog("Starting TCP server...")

    for {

		conn, err := server.Accept()
		
		fmt.Println("connection accepted...")
		
        if err != nil {
           	go WriteLog("Error accepting: "+err.Error())
            continue
		}

		var messageQueue = make(chan string, *ConfigTCPObj.Server.TCP.BufferRead)

		defer close(messageQueue)
		
		go RecieveMessage(conn, messageQueue)

		go HandleRequest(conn, messageQueue)
	}
}