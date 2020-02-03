package server

import (
	"pojo"
	"net"
	"fmt"
	"os"
	"time"
)

type ServerConnection struct{
	connections map[net.Conn] time.Time
}

var File *os.File

func HostTCP(configObj pojo.Config){

	server, err := net.Listen("tcp", *configObj.Server.Host +":"+ *configObj.Server.Port)

    if err != nil {
        fmt.Println("Error listening:", err.Error())
        os.Exit(1)
	}
	
	defer server.Close()

	fmt.Println("Listening on " + *configObj.Server.Host + ":" + *configObj.Server.Port+"...")

	File, err = os.OpenFile("D:/brahmaputra/log.out", os.O_APPEND|os.O_WRONLY, 0600)

	if err != nil {
		fmt.Println(err)
	}

	defer File.Close()

    for {

		// time.Sleep(1)

		conn, err := server.Accept()

		fmt.Println("connection accepted...")
		
        if err != nil {
            fmt.Println("Error accepting: ", err.Error())
		}

		var messageQueue = make(chan string)

		defer close(messageQueue)
		
		go RecieveMessage(conn, messageQueue)

		go HandleRequest(conn, messageQueue)
		
	}
}
