package keeper

import (
	"net"
	"fmt"
	"os"
)

func HostTCPServer(tcpNode map [string]interface{}){

	server, err := net.Listen("tcp", tcpNode["host"].(string) +":"+ tcpNode["port"].(string))

    if err != nil {
        WriteLog("Error listening: "+err.Error())
        os.Exit(1)
	}

	defer server.Close()

	fmt.Println("Listening on " + tcpNode["host"].(string) + ":" + tcpNode["port"].(string)+"...")

	go ConnectTCPClusters()

	for {

		conn, err := server.Accept()

		fmt.Println("connection accepted...")
		
        if err != nil {
           	go WriteLog("Error accepting: "+err.Error())
            continue
		}

		var messageQueue = make(chan string)

		defer close(messageQueue)

		go RecieveMessage(conn, messageQueue)

		go HandleRequest(conn, messageQueue)
	}
}