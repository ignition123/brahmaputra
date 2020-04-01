package brahmaputra

import(
	"time"
	"log"
	"net"
	"net/url"
)

func (e *CreateProperties) connectUDP(){

	defer handlepanic()

	if e.PoolSize > 0{

		var connectStatus = true

		for i := 0; i < e.PoolSize; i++ {

			e.Conn = e.createUDPConnection()

			if e.Conn == nil{

				connectStatus = false

				break

			}

			e.ConnPool = append(e.ConnPool, e.Conn)	

		}

		if !connectStatus{

			time.Sleep(2 * time.Second)

			e.connectTCP()

		}

	}else{

		e.Conn = e.createUDPConnection()

		if e.Conn == nil{

			time.Sleep(2 * time.Second)

			e.connectTCP()

			return

		}

		e.ConnPool = append(e.ConnPool, e.Conn)	

	}

	if len(e.requestPull) > 0{

		var chancb = make(chan bool, 1)

		for _, bodyMap := range e.requestPull{

			go e.Publish(bodyMap, chancb)

			<-chancb
		}

	}

	if e.AppType == "producer"{

		go log.Println("Application started as producer...")
	}

	if e.AppType == "consumer"{

		go log.Println("Application started as consumer...")

		if e.PoolSize > 0{

			for index := range e.ConnPool{

				go e.receiveSubMsg(e.ConnPool[index])

			}

		}else{

			go e.receiveSubMsg(e.ConnPool[0])

		}

		if e.subReconnect{

			for{

				time.Sleep(1 * time.Second)

				if e.Conn != nil{
					break
				}
			}

			e.Subscribe(e.contentMatcher)
		}
	} 

	e.connectStatus = true

	if e.AuthReconnect{

		go e.checkUDPConnectStatus()
		
	}

}

func (e *CreateProperties) checkUDPConnectStatus(){

	defer handlepanic()

	for{

		time.Sleep(2 * time.Second)

		if e.connectStatus == false{

			e.ConnPool = e.ConnPool[:0]

			e.subscribeFD.Close()

			e.connectUDP()

			break

		}
	}

}

func (e *CreateProperties) createUDPConnection() net.Conn{

	defer handlepanic()

	var conn net.Conn

	var err error

	url, err := url.Parse(e.Url)

    if err != nil {
        go log.Println(err)
        return nil
    }

	host, port, _ := net.SplitHostPort(url.Host)

	dest := host + ":" + port

	if e.ConnectionType != "tcp" && e.ConnectionType != "udp"{

		conn, err = net.Dial("tcp", dest)

	}else if e.ConnectionType == "tcp"{

		conn, err = net.Dial("tcp", dest)

	}else if e.ConnectionType == "udp"{

		RemoteAddr, err := net.ResolveUDPAddr("udp", dest)

		if err != nil{

			go log.Println(err)

			return nil

		}

		conn, err = net.DialUDP("udp", nil, RemoteAddr)
	}


	if err != nil{

		go log.Println(err)

		return nil
	}

	conn.(*net.UDPConn).SetReadBuffer(1000000000)
	conn.(*net.UDPConn).SetWriteBuffer(1000000000)
	conn.(*net.UDPConn).SetDeadline(time.Now().Add(1000000 * time.Second))
	conn.(*net.UDPConn).SetReadDeadline(time.Now().Add(1000000 * time.Second))
	conn.(*net.UDPConn).SetWriteDeadline(time.Now().Add(1000000 * time.Second))

	return conn

}