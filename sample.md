    // setting the log file where all the messages will be written
    
    var socketFilePath = "/path/of/the/socket.log"
        
    // opening a file in append mode
    
    FileDescriptor, err := os.OpenFile(socketFilePath,
			os.O_APPEND|os.O_WRONLY, 0700)

    // checking for errors while opening file
    if err != nil {
        go log.Write(err.Error())
        return
    }
    
    // Host TCP server in localhost with port 8100
    
    server, err := net.Listen("tcp", "127.0.0.1:8100")

         
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
      
      // subscribe to the channel file
      go SubscribeChannel(*tcp)
      
      // calling handleRequest using goroutines to run in seperate thread
      go HandleRequest(*tcp)
    }
    
    func SubscribeChannel(conn net.TCPConn){
        
        // opening file 
        file, err := os.Open(socketFilePath)
        
        // defering the file to close, it will be closed only when loop is exited
        defer file.Close()

        if err != nil {
            go log.Write(err.Error())
            return
        }
              
        // creating a file reader
        reader := bufio.NewReader(file)
        
        var line string
        
        var err error
        
        var lastModifiedDate int64
        
        for{
            
            // getting file status
            fileStat, err := os.Stat(socketFilePath)
 
            if err != nil {
                go log.Write(err.Error())
                break
            }
            
            // checking for last modified time
            if fileStat.ModTime() == lastModifiedDate{
                thread.Sleep(1 * time.Nanosecond)
                continue
            }
            
            // reading lines
            line, err = reader.ReadString('\n')
            
            if err != nil {
                if err == io.EOF {
                    continue
                }else{
                    break
                }
            }
            
            //updating the last modified time
            lastModifiedDate = fileStat.ModTime()
            
            // setting a variable to count number of retry
            var totalRetry = 0
        
            RETRY:
            
            // if retry is already 3 then exit loop
            if totalRetry > 3{
                log.Println("Socket disconnected...")
                break
            }
            // writing to clients
            _, socketError := conn.Write([]byte(line))
            
            // if failed then retry again till count = 3
            if socketError != nil{
                totalRetry += 1
                goto RETRY
            }
            
        }
        
    }
    
    func HandleRequest(conn net.TCPConn){
      
      // waiting for new messages
      
      var callbackchannels = make(chan bool)
      
      for {
          
          // reading the message from socket buffer
          
          message, err := bufio.NewReader(conn).ReadString('\n')
          
          if err != nil {
              log.Printf("Error: %+v", err.Error())
              return
          }
          
          // broadcasting to all clients
          go WriteData(message, callbackchannels)
          
          <-callbackchannels
      }
    }
    
    
    func WriteData(message string, callback chan bool){
        
        mtx.Lock()
        
        _, err := FileDescriptor.Write(message)
        
        mtx.Unlock()
        
        if (err != nil && err != io.EOF ){
            go log.Write(err.Error())
            callback <- false
            return
        }
        
        callback <- true
        
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
