package server

import (
	"pojo"
	"net"
	"log"
	"os"
	"time"
	"ChannelList"
	"server/tcp"
)

func HostTCP(configObj pojo.Config){

	defer ChannelList.Recover()

	ChannelList.ConfigTCPObj = configObj

	go tcp.ChannelMethod.GetChannelData()

	if *ChannelList.ConfigTCPObj.Server.TCP.Host != "" && *ChannelList.ConfigTCPObj.Server.TCP.Port != ""{
		HostTCPServer()
	}
}

func HostTCPServer(){

	defer ChannelList.Recover()

	ChannelList.SetUlimit()

	serverObject, err := net.Listen("tcp", *ChannelList.ConfigTCPObj.Server.TCP.Host +":"+ *ChannelList.ConfigTCPObj.Server.TCP.Port)

    if err != nil {
        ChannelList.WriteLog("Error listening: "+err.Error())
        os.Exit(1)
	}
	
	defer serverObject.Close()

	log.Println("Listening on " + *ChannelList.ConfigTCPObj.Server.TCP.Host + ":" + *ChannelList.ConfigTCPObj.Server.TCP.Port+"...")

	tcp.LoadTCPChannelsToMemory()


	ChannelList.WriteLog("Loading log files...")
	ChannelList.WriteLog("Starting TCP server...")

    for {

		conn, err := serverObject.Accept()
		
		log.Println("connection accepted...")
		
        if err != nil {
           	go ChannelList.WriteLog("Error accepting: "+err.Error())
            continue
		}

		tcpObject := conn.(*net.TCPConn)

        tcpObject.SetNoDelay(true)
        tcpObject.SetKeepAlive(true)
		tcpObject.SetLinger(1)
		tcpObject.SetReadBuffer(10000)
		tcpObject.SetWriteBuffer(10000)
		tcpObject.SetDeadline(time.Now().Add(1000000 * time.Second))
		tcpObject.SetReadDeadline(time.Now().Add(1000000 * time.Second))
		tcpObject.SetWriteDeadline(time.Now().Add(1000000 * time.Second))

		go tcp.HandleRequest(*tcpObject)
	}
}

