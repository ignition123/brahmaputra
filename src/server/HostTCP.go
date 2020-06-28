package server

import (
	"pojo"
	"net"
	"log"
	"os"
	"time"
	"ChannelList"
	"server/tcp"
	"runtime"
)

func HostTCP(configObj pojo.Config){

	defer ChannelList.Recover()

	// loading the config object

	ChannelList.ConfigTCPObj = configObj

	// loading all configurations to inmemory

	tcp.LoadTCPChannelsToMemory()

	// checking for host and port

	if *ChannelList.ConfigTCPObj.Server.TCP.Host != "" && *ChannelList.ConfigTCPObj.Server.TCP.Port != ""{

		// starting the tcp server

		HostTCPServer()
	}
}

func HostTCPServer(){

	defer ChannelList.Recover()

	// changing the ulimit, it works in linux and mac kindly uncomment the code

	ChannelList.SetUlimit()

	// lisetning to the host ip and port

	serverObject, err := net.Listen("tcp", *ChannelList.ConfigTCPObj.Server.TCP.Host +":"+ *ChannelList.ConfigTCPObj.Server.TCP.Port)

    if err != nil {
        ChannelList.WriteLog("Error listening: "+err.Error())
        os.Exit(1)
	}
	
	log.Println("Listening on " + *ChannelList.ConfigTCPObj.Server.TCP.Host + ":" + *ChannelList.ConfigTCPObj.Server.TCP.Port+"...")

	ChannelList.WriteLog("Loading log files...")
	ChannelList.WriteLog("Starting Brahmaputra TCP server...")

	// start accepting for new sockets

	for i := 0; i < runtime.NumCPU(); i++{

		go acceptSocket(serverObject)

	}

    ChannelList.WriteLog("Started Brahmaputra TCP server...")
}

func acceptSocket(serverObject net.Listener){

	defer serverObject.Close()

	// waiting for new sockets
	
	for {

		conn, err := serverObject.Accept()
		
		log.Println("connection accepted...")
		
        if err != nil {
           	go ChannelList.WriteLog("Error accepting: "+err.Error())
            continue
		}

		tcpObject := conn.(*net.TCPConn)

		// checking for nodelay flag of tcp

		if *ChannelList.ConfigTCPObj.Server.TCP.NoDelay != false{
			tcpObject.SetNoDelay(*ChannelList.ConfigTCPObj.Server.TCP.NoDelay)
		}

		// checking for keepalive flag

		if *ChannelList.ConfigTCPObj.Server.TCP.KeepAlive != false{
			tcpObject.SetKeepAlive(*ChannelList.ConfigTCPObj.Server.TCP.KeepAlive)
		}

		// checking for linger
        
        if *ChannelList.ConfigTCPObj.Server.TCP.Linger != 0{
        	tcpObject.SetLinger(*ChannelList.ConfigTCPObj.Server.TCP.Linger)
        }

        // checking for read and write timeout
        
        if *ChannelList.ConfigTCPObj.Server.TCP.Timeout != 0{
        	tcpObject.SetDeadline(time.Now().Add(time.Duration(*ChannelList.ConfigTCPObj.Server.TCP.Timeout) * time.Millisecond))
        }

        // checking for read timeout

        if *ChannelList.ConfigTCPObj.Server.TCP.SocketReadTimeout != 0{
        	tcpObject.SetReadDeadline(time.Now().Add(time.Duration(*ChannelList.ConfigTCPObj.Server.TCP.SocketReadTimeout) * time.Millisecond))
        }
			
		// checking for write timeout

		if *ChannelList.ConfigTCPObj.Server.TCP.SocketWriteTimeout != 0{
			tcpObject.SetWriteDeadline(time.Now().Add(time.Duration(*ChannelList.ConfigTCPObj.Server.TCP.SocketWriteTimeout) * time.Millisecond))
		}
		
		// handling the new socket

		go tcp.HandleRequest(*tcpObject)
	}
	
}