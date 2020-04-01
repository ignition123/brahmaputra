package brahmaputra

import(
	"time"
	"log"
	"net"
	"net/url"
)

func (e *CreateProperties) connectTCP(){

	defer handlepanic()

	if e.PoolSize > 0{

		var connectStatus = true

		for i := 0; i < e.PoolSize; i++ {

			e.Conn = e.createTCPConnection()

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

		e.Conn = e.createTCPConnection()

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

		if e.Acknowledge{

			if e.PoolSize > 0{

				for index := range e.ConnPool{

					go e.receiveMsg(e.ConnPool[index])

				}

			}else{

				go e.receiveMsg(e.ConnPool[0])

			}

		}
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

		go e.checkTCPConnectStatus()
		
	}
}

func (e *CreateProperties) checkTCPConnectStatus(){

	defer handlepanic()

	for{

		time.Sleep(2 * time.Second)

		if e.connectStatus == false{

			e.ConnPool = e.ConnPool[:0]

			e.subscribeFD.Close()

			e.connectTCP()

			break

		}
	}

}

func (e *CreateProperties) createTCPConnection() net.Conn{

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

		conn, err = net.Dial("udp", dest)

	}

	if err != nil{

		go log.Println(err)

		return nil
	}

	conn.(*net.TCPConn).SetKeepAlive(true)
	conn.(*net.TCPConn).SetLinger(1)
	conn.(*net.TCPConn).SetNoDelay(true)
	conn.(*net.TCPConn).SetReadBuffer(10000)
	conn.(*net.TCPConn).SetWriteBuffer(10000)
	conn.(*net.TCPConn).SetDeadline(time.Now().Add(1000000 * time.Second))
	conn.(*net.TCPConn).SetReadDeadline(time.Now().Add(1000000 * time.Second))
	conn.(*net.TCPConn).SetWriteDeadline(time.Now().Add(1000000 * time.Second))

	return conn
}