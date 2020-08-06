package server

import (
	"objects"
	"net"
	"log"
	"os"
	"time"
	"ChannelList"
	"server/tcp"
	"runtime"
)

func HostTCP(configObj objects.Config){

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

		// checking for nodelay flag of tcp

		if *ChannelList.ConfigTCPObj.Server.TCP.NoDelay != false{
			conn.(*net.TCPConn).SetNoDelay(*ChannelList.ConfigTCPObj.Server.TCP.NoDelay)
		}

		// checking for keepalive flag

		if *ChannelList.ConfigTCPObj.Server.TCP.KeepAlive != false{
			conn.(*net.TCPConn).SetKeepAlive(*ChannelList.ConfigTCPObj.Server.TCP.KeepAlive)
		}

		// checking for linger
        
        if *ChannelList.ConfigTCPObj.Server.TCP.Linger != 0{
        	conn.(*net.TCPConn).SetLinger(*ChannelList.ConfigTCPObj.Server.TCP.Linger)
        }

        // checking for read and write timeout
        
        if *ChannelList.ConfigTCPObj.Server.TCP.Timeout != 0{

        	conn.(*net.TCPConn).SetDeadline(time.Now().Add(time.Duration(*ChannelList.ConfigTCPObj.Server.TCP.Timeout) * time.Millisecond))

        }else{

        	conn.(*net.TCPConn).SetDeadline(time.Time{})
        }

		// setting read buffer size

		if *ChannelList.ConfigTCPObj.Server.TCP.ReadBuffer != 0{

			conn.(*net.TCPConn).SetReadBuffer(*ChannelList.ConfigTCPObj.Server.TCP.ReadBuffer)

		}

		// setting write buffer size

		if *ChannelList.ConfigTCPObj.Server.TCP.WriteBuffer != 0{

			conn.(*net.TCPConn).SetWriteBuffer(*ChannelList.ConfigTCPObj.Server.TCP.WriteBuffer)

		}
		
		// handling the new socket

		go tcp.HandleRequest(conn)
	}
	
}