package server


import(
	"net"
	"fmt"
	"time"
)

func RecieveMessage(conn net.Conn, messageQueue chan string){

	var stopIterate = false

	for{

		if stopIterate{
			break
		}

		time.Sleep(0.1)

		select {
			case val, ok := <-messageQueue:
				if ok{

					if val == "BRAHMAPUTRA_DISCONNECT"{
						break
					}

					go ParseMsg(val, conn)
    
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