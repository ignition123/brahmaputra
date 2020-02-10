package server


import(
	"net"
	"context"
)

func RecieveMessage(conn net.Conn, messageQueue chan string){

	var stopIterate = false

	ctx := context.Background()

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
			break
			case <-ctx.Done():
				go WriteLog("Channel closed...")
		}
	}
}