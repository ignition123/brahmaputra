package brahmaputra

/*
	Connecting the TCP sockets
*/

// importing modules

import(
	"time"
	"log"
	"net"
	"net/url"
)

// connecting the tcp

func (e *CreateProperties) connectTCP(){

	defer handlepanic()

	// checking pool size if greater than 0

	if e.PoolSize > 0{

		// setting connection status as true by default

		connectStatus := true

		// iterating over the pool size 

		for i := 0; i < e.PoolSize; i++ {

			// creating tcp connections

			e.Conn = e.createTCPConnection()

			// if connection is not successful then connectionStatus is set to false

			if e.Conn == nil{

				connectStatus = false

				break

			}

			// appending all connection object to connection pool array

			e.ConnPool = append(e.ConnPool, e.Conn)	

		}

		// if connectionStatus == false then it will attempt to reconnect after 2 seconds

		if !connectStatus{

			time.Sleep(2 * time.Second)

			e.connectTCP()

		}

	}else{

		// if there is no pool size set then it will create a single tcp connection

		e.Conn = e.createTCPConnection()

		// if connection is failure then it will attempt to reconnect to tcp after every 2 seconds

		if e.Conn == nil{

			time.Sleep(2 * time.Second)

			e.connectTCP()

			return

		}

		// appending the single tcp connection to the pool array

		e.ConnPool = append(e.ConnPool, e.Conn)	

	}

	// checking the length of requestPull array if greater than 0

	if len(e.requestPull) > 0{

		// creating a boolean channel

		chancb := make(chan bool, 1)
		defer close(chancb)

		// after reconnection if any message are pending in the request pull then it will publish message to the server again

		for _, bodyMap := range e.requestPull{

			go e.Publish(bodyMap, chancb)

			// waiting for callbacks

			<-chancb
		}

	}

	// if app type == producer then 

	if e.AppType == "producer"{

		go log.Println("Application started as producer...")

		// checking if acknowledgment is True

		if e.Acknowledge{

			// if pool size greater than 0 then receiving ack message from all the pool connections

			if e.PoolSize > 0{

				for index := range e.ConnPool{

					go e.receiveMsg(e.ConnPool[index])

				}

			}else{

				// receiving ack message from single tcp connection

				go e.receiveMsg(e.ConnPool[0])

			}

		}
	}

	// app type is consumer then 

	if e.AppType == "consumer"{

		go log.Println("Application started as consumer...")

		// if pool size greater than 0 then receiving message from all the pool connections

		if e.PoolSize > 0{

			for index := range e.ConnPool{

				go e.receiveSubMsg(e.ConnPool[index])

			}

		}else{

			// receiving message from single tcp connection

			go e.receiveSubMsg(e.ConnPool[0])

		}

		// checking if subscriber content matcher flag is true then, it will match the packet with the content matcher if true then pass ele discard message

		if e.subContentmatcher{

			for{

				time.Sleep(1 * time.Second)

				if e.Conn != nil{
					break
				}
			}

			e.Subscribe(e.contentMatcher)
		}
	} 

	// marking the connectStatus as true after successfull connection

	e.connectStatus = true

	// if auto reconnect flag is true then it will start listening for disconnection of tcp socket

	if e.AuthReconnect{

		go e.checkTCPConnectStatus()
		
	}
}

// method to check if any sort of disconnection in tcp socket, attempting to reconnect

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

// connecting tcp connection

func (e *CreateProperties) createTCPConnection() net.Conn{

	defer handlepanic()

	var conn net.Conn

	var err error

	url, err := url.Parse(e.Url)

    if err != nil {
        go log.Println(err)
        return nil
    }

    // splitting the url into host and port

	host, port, _ := net.SplitHostPort(url.Host)

	dest := host + ":" + port

	// connecting to tcp socket

	if e.ConnectionType == "tcp"{
		
		conn, err = net.Dial("tcp", dest)

	}else{

		conn, err = net.Dial("tcp", dest)
	}

	// chekcing for error while connecting the tcp

	if err != nil{

		go log.Println(err)

		return nil
	}

	// setting some default tcp flags and connection

	conn.(*net.TCPConn).SetKeepAlive(true)
	conn.(*net.TCPConn).SetLinger(1)
	conn.(*net.TCPConn).SetNoDelay(true)
	conn.(*net.TCPConn).SetDeadline(time.Now().Add(1000000 * time.Second))
	conn.(*net.TCPConn).SetReadDeadline(time.Now().Add(1000000 * time.Second))
	conn.(*net.TCPConn).SetWriteDeadline(time.Now().Add(1000000 * time.Second))

	return conn
}