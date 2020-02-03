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

func HostTCP(configObj pojo.Config){

	LoadTCPChannelsToMemory()

	GetChannelData()

	if *configObj.Server.TCP.Host != "" && *configObj.Server.TCP.Port != ""{
		HostTCPServer(configObj)
	}
}

func HostTCPServer(configObj pojo.Config){

	server, err := net.Listen("tcp", *configObj.Server.TCP.Host +":"+ *configObj.Server.TCP.Port)

    if err != nil {
        fmt.Println("Error listening:", err.Error())
        WriteLog(err.Error())
        os.Exit(1)
	}
	
	defer server.Close()

	fmt.Println("Listening on " + *configObj.Server.TCP.Host + ":" + *configObj.Server.TCP.Port+"...")

    for {

		conn, err := server.Accept()

		fmt.Println("connection accepted...")
		
        if err != nil {
            fmt.Println("Error accepting: ", err.Error())
           	WriteLog(err.Error())
            continue
		}

		var messageQueue = make(chan string)

		defer close(messageQueue)
		
		go RecieveMessage(conn, messageQueue)

		go HandleRequest(conn, messageQueue)

		fmt.Println("Loading log files...")
		fmt.Println("Starting TCP server...")
	}
}