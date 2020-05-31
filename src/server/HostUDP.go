package server

import (
	"pojo"
	"net"
	"log"
	"os"
	"time"
	"ChannelList"
	"server/udp"
)


func HostUDP(configObj pojo.Config){

	defer ChannelList.Recover()

	ChannelList.ConfigUDPObj = configObj

	udp.LoadUDPChannelsToMemory()

	if *ChannelList.ConfigUDPObj.Server.UDP.Host != "" && *ChannelList.ConfigUDPObj.Server.UDP.Port != ""{
		HostUDPServer()
	}
}


func HostUDPServer(){

	defer ChannelList.Recover()

	ChannelList.SetUlimit()

	udpAddr, err := net.ResolveUDPAddr("udp4", *ChannelList.ConfigUDPObj.Server.UDP.Host +":"+ *ChannelList.ConfigUDPObj.Server.UDP.Port)

	if err != nil {
	    ChannelList.WriteLog("Error listening: "+err.Error())
	    os.Exit(1)
	}

	serverObject, listenErr := net.ListenUDP("udp", udpAddr)

    if listenErr != nil {
        ChannelList.WriteLog("Error listening: "+listenErr.Error())
        os.Exit(1)
    }

	defer serverObject.Close()

	log.Println("Listening on " + *ChannelList.ConfigUDPObj.Server.UDP.Host +":"+ *ChannelList.ConfigUDPObj.Server.UDP.Port+"...")

	ChannelList.WriteLog("Loading log files...")
	ChannelList.WriteLog("Starting UDP server...")

	serverObject.SetReadBuffer(1000000000)
	serverObject.SetWriteBuffer(1000000000)
	serverObject.SetDeadline(time.Now().Add(1000000 * time.Second))
	serverObject.SetReadDeadline(time.Now().Add(1000000 * time.Second))
	serverObject.SetWriteDeadline(time.Now().Add(1000000 * time.Second))

	for {	

        udp.HandleRequest(serverObject)

 	}

}