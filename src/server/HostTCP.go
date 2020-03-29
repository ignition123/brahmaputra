package server

import (
	"pojo"
	"net"
	"log"
	"os"
	"time"
	"ChannelList"
)

type ServerTCPConnection struct{
	connections map[net.Conn] time.Time
}

var ChannelMethod = &ChannelMethods{}

func HostTCP(configObj pojo.Config){

	defer ChannelList.Recover()

	ChannelList.ConfigTCPObj = configObj

	LoadTCPChannelsToMemory()

	go ChannelMethod.GetChannelData()

	if *ChannelList.ConfigTCPObj.Server.TCP.Host != "" && *ChannelList.ConfigTCPObj.Server.TCP.Port != ""{
		HostTCPServer()
	}
}

func HostTCPServer(){

	defer ChannelList.Recover()

	server, err := net.Listen("tcp", *ChannelList.ConfigTCPObj.Server.TCP.Host +":"+ *ChannelList.ConfigTCPObj.Server.TCP.Port)

    if err != nil {
        ChannelList.WriteLog("Error listening: "+err.Error())
        os.Exit(1)
	}
	
	defer server.Close()

	log.Println("Listening on " + *ChannelList.ConfigTCPObj.Server.TCP.Host + ":" + *ChannelList.ConfigTCPObj.Server.TCP.Port+"...")

	ChannelList.WriteLog("Loading log files...")
	ChannelList.WriteLog("Starting TCP server...")

    for {

		conn, err := server.Accept()
		
		log.Println("connection accepted...")
		
        if err != nil {
           	go ChannelList.WriteLog("Error accepting: "+err.Error())
            continue
		}

		tcp := conn.(*net.TCPConn)

        tcp.SetNoDelay(true)
        tcp.SetKeepAlive(true)
		tcp.SetLinger(1)
		tcp.SetReadBuffer(10000)
		tcp.SetWriteBuffer(10000)
		tcp.SetDeadline(time.Now().Add(1000000 * time.Second))
		tcp.SetReadDeadline(time.Now().Add(1000000 * time.Second))
		tcp.SetWriteDeadline(time.Now().Add(1000000 * time.Second))

		go HandleRequest(*tcp)
	}
}