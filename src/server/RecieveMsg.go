package server


import(
	"net"
	_"time"
	"fmt"
)

func RecieveMessage(conn net.Conn, messageQueue chan string){

	var stopIterate = false

	for{

		// time.Sleep(1)

		if stopIterate{
			break
		}

		select {
			case val, ok := <-messageQueue:
				if ok{

					if val == "BRAHMAPUTRA_DISCONNECT"{
						break
					}

					ParseMsg(val, conn)
    
				}else{
					fmt.Println("Connection closed!")
					fmt.Println("Channel closed!")
					stopIterate = true
					break
				
				}
			default:
				//fmt.Println("Waiting for messagses...")
		}
	}
}