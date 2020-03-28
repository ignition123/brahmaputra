package keeper


import(
	"net"
)

func RecieveMessage(conn net.Conn, messageQueue chan string){

	var stopIterate = false

	for{

		if stopIterate{
			break
		}

		select {
			case val, ok := <-messageQueue:
				if ok{

					if val == "BRAHMAPUTRA_DISCONNECT"{
						break
					}

					go ParseMsg(val, conn)
    
				}else{
					WriteLog("Connection closed!")
					WriteLog("Channel closed!")
					stopIterate = true
					break
				
				}
			default:
				//fmt.Println("Waiting for messagses...")
		}
	}
}