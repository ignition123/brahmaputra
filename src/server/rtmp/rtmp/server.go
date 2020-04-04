package rtmp

import (
	"fmt"
	"net"
	"runtime"
	"sync"
	"time"
	"ChannelList"
)

var begintime time.Time

var handler ServerHandler = new(DefaultServerHandler)

type Server struct{

	Addr        string
	Handler     ServerHandler
	ReadTimeout time.Duration
	WriteTimout time.Duration
	Lock        *sync.Mutex

}

func ListenAndServe(addr string) error{

	s := Server{
		Addr:        addr,                            
		Handler:     handler,                         
		ReadTimeout: time.Duration(time.Second * 15),
		WriteTimout: time.Duration(time.Second * 15), 
		Lock:        new(sync.Mutex)} 
		         
	return s.ListenAndServer()

}


func (s *Server) ListenAndServer() error{

	addr := s.Addr

	if addr == ""{

		addr = ":1935"

	}

	l, err := net.Listen("tcp", addr)

	if err != nil{

		return err

	}

	for i := 0; i < runtime.NumCPU(); i++{

		go s.loop(l)

	}

	return nil
}

func (s *Server) loop(listener net.Listener) error{

	defer listener.Close()

	var tempDelay time.Duration

	for {

		conn, err := listener.Accept()

		if err != nil{

			if ne, ok := err.(net.Error); ok && ne.Temporary(){

				if tempDelay == 0{

					tempDelay = 5 * time.Millisecond

				}else{

					tempDelay *= 2

				}

				if max := 1 * time.Second; tempDelay > max{

					tempDelay = max

				}

				fmt.Printf("rtmp: Accept error: %v; retrying in %v", err, tempDelay)

				time.Sleep(tempDelay)

				continue

			}

			return err
		}

		tempDelay = 0

		c := newRtmpNetConnect(conn, s)

		go s.serve(c)
	}
}

func (s *Server) serve(rtmpNetConn *RtmpNetConnection){

	begintime = time.Now()

	err := handshake(rtmpNetConn.brw)

	if err != nil {

		rtmpNetConn.Close()
		return

	}

	msg, err := recvMessage(rtmpNetConn)

	if err != nil{

		rtmpNetConn.Close()
		return

	}

	connect, ok := msg.(*ConnectMessage)

	if !ok || connect.CommandName != "connect"{

		fmt.Println("not recv rtmp connet message")
		rtmpNetConn.Close()
		return

	}

	data := decodeAMFObject(connect.Object, "app")

	if data != nil{

		rtmpNetConn.appName, ok = data.(string)

		if !ok {

			fmt.Println("rtmp connet message <app> decode error")
			rtmpNetConn.Close()
			return

		}

		if ChannelList.RTMPStorage[rtmpNetConn.appName] == nil{

			fmt.Println("Invalid channel name : "+rtmpNetConn.appName)
			rtmpNetConn.Close()
			return

		}

	}

	data = decodeAMFObject(connect.Object, "objectEncoding")

	if data != nil{

		rtmpNetConn.objectEncoding, ok = data.(float64)

		if !ok {
			fmt.Println("rtmp connet message <objectEncoding> decode error")
			rtmpNetConn.Close()
			return
		}

	}

	err = sendMessage(rtmpNetConn, SEND_ACK_WINDOW_SIZE_MESSAGE, uint32(512<<10))

	if err != nil{

		rtmpNetConn.Close()
		return

	}

	err = sendMessage(rtmpNetConn, SEND_SET_PEER_BANDWIDTH_MESSAGE, uint32(512<<10))

	if err != nil{

		rtmpNetConn.Close()
		return

	}

	err = sendMessage(rtmpNetConn, SEND_STREAM_BEGIN_MESSAGE, nil)

	if err != nil{

		rtmpNetConn.Close()
		return

	}

	crmd := newConnectResponseMessageData(rtmpNetConn.objectEncoding)

	err = sendMessage(rtmpNetConn, SEND_CONNECT_RESPONSE_MESSAGE, crmd)

	if err != nil{

		rtmpNetConn.Close()
		return

	}

	rtmpNetConn.connected = true

	handler := s.Handler

	newNetStream(rtmpNetConn, handler).msgLoopProc()
}
