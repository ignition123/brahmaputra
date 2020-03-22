    // Host TCP server in localhost with port 8100
    
    server, err := net.Listen("tcp", "127.0.0.1:8100")
     
    // create array of tcp socket client
    var listOfClients []*net.TCPConn
    
    // waiting for new connections
    for {
      conn, err := server.Accept()
      
      tcp := conn.(*net.TCPConn)

      tcp.SetNoDelay(true)
      tcp.SetKeepAlive(true)
      tcp.SetKeepAlive(true)
      tcp.SetLinger(1)
      tcp.SetReadBuffer(10000)
      tcp.SetWriteBuffer(10000)
      tcp.SetDeadline(time.Now().Add(1000000 * time.Second))
      tcp.SetReadDeadline(time.Now().Add(1000000 * time.Second))
      tcp.SetWriteDeadline(time.Now().Add(1000000 * time.Second))
      
      // appending the client socket to array/slice
      listOfClients = append(listOfClients, tcp)
      
      // calling handleRequest using goroutines to run in seperate thread
      go HandleRequest(*tcp)
    }
    
    func HandleRequest(conn net.TCPConn){
      
      // waiting for new messages
      
      for {
          
          // reading the message from socket buffer
          
          message, err := bufio.NewReader(conn).ReadString('\n')
          
          if err != nil {
              log.Printf("Error: %+v", err.Error())
              return
          }
          
          // broadcasting to all clients
          go broadCast(message)
      }
    }
    
    
    func broadCast(message string){
    
      // iterating over the listOfClients
      
      for i := range listOfClients{
        // sending message to all clients in the socket
        listOfClients[i].Write(message)
      }
    }
    
    ###################################################################################
    
    // Client Socket example
    
    // Client A connect to this socket
    
    // connecting to localhost with port 8100
    conn, _ := net.Dial("tcp", "127.0.0.1:8100")
    
    // writing message to socket
    
    conn.Write([]byte("Message from client A"))
    
    for { 
      // reading messages from server
      message, err := bufio.NewReader(conn).ReadString('\n')
      if err != nil {
          log.Printf("Error: %+v", err.Error())
          return
      }
      
      fmt.Println(message)
          
    }
    
      // Same code for another client
     // Client B connect to this socket
    
    conn, _ := net.Dial("tcp", "127.0.0.1:8100")
    
    conn.Write([]byte("Message from client B"))
    
    for { 
      
      message, err := bufio.NewReader(conn).ReadString('\n')
      if err != nil {
          log.Printf("Error: %+v", err.Error())
          return
      }
      
      fmt.Println(message)
          
    }
