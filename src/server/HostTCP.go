package server

import (
	"pojo"
	"net"
	"fmt"
	"os"
	"time"
	"MongoConnection"
	"ChannelList"
)

type ServerTCPConnection struct{
	connections map[net.Conn] time.Time
}

var closeTCP = false
var TCPTotalConnection = 0

func HostTCP(configObj pojo.Config){

	defer ChannelList.Handlepanic()

	ChannelList.ConfigTCPObj = configObj

	if !ConnectStorage(){
		ChannelList.WriteLog("Unable to connect to storage...")
		return
	}

	LoadTCPChannelsToMemory()

	GetChannelData()

	if *ChannelList.ConfigTCPObj.Server.TCP.Host != "" && *ChannelList.ConfigTCPObj.Server.TCP.Port != ""{
		HostTCPServer()
	}
}

func HostTCPServer(){

	defer ChannelList.Handlepanic()

	server, err := net.Listen("tcp", *ChannelList.ConfigTCPObj.Server.TCP.Host +":"+ *ChannelList.ConfigTCPObj.Server.TCP.Port)

    if err != nil {
        ChannelList.WriteLog("Error listening: "+err.Error())
        os.Exit(1)
	}
	
	defer server.Close()

	fmt.Println("Listening on " + *ChannelList.ConfigTCPObj.Server.TCP.Host + ":" + *ChannelList.ConfigTCPObj.Server.TCP.Port+"...")

	ChannelList.WriteLog("Loading log files...")
	ChannelList.WriteLog("Starting TCP server...")

    for {

    	if closeTCP{
    		server.Close()
    		return
    	}

		conn, err := server.Accept()
		
		fmt.Println("connection accepted...")
		
        if err != nil {
           	go ChannelList.WriteLog("Error accepting: "+err.Error())
            continue
		}

		var messageQueue = make(chan string)
		
		go RecieveMessage(conn, messageQueue)

		go HandleRequest(conn, messageQueue)
	}
}

func CloseTCPServers(){

	defer ChannelList.Handlepanic()

	fmt.Println("Closing tcp socket...")
	closeTCP = true
}

func ConnectStorage() bool{

	defer ChannelList.Handlepanic()

	if *ChannelList.ConfigTCPObj.Storage.Mongodb.Active{
		
		if(!MongoConnection.Connect()){
			
			fmt.Println("Failed to connect Mongodb")

			return false
		}

		if !MongoConnection.SetupCollection(){
			return false
		}

	}
	
	return true

}