package keeper

import (
	"pojo"
	"net"
	"fmt"
	"os"
)

func HostTCPServer(configObj pojo.KeeperStruct){

	server, err := net.Listen("tcp", *configObj.Host +":"+ *configObj.Port)

    if err != nil {
        WriteLog("Error listening: "+err.Error())
        os.Exit(1)
	}

	defer server.Close()

	fmt.Println("Listening on " + *configObj.Host + ":" + *configObj.Port+"...")

	go ConnectTCPClusters(configObj)

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