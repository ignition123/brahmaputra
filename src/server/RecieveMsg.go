package server


import(
	"net"
	"context"
	"ChannelList"
	"strconv"
)

func RecieveMessage(conn net.Conn, messageQueue chan string){

	var stopIterate = false

	ctx := context.Background()

	TCPTotalConnection += 1

	ChannelList.WriteLog("Total connection now open: "+strconv.Itoa(TCPTotalConnection))

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
					ChannelList.WriteLog("Connection closed!")
					ChannelList.WriteLog("Channel closed!")
					TCPTotalConnection -= 1
					ChannelList.WriteLog("Total connection now open: "+strconv.Itoa(TCPTotalConnection))
					stopIterate = true
					break
				
				}
			break
			case <-ctx.Done():
				go ChannelList.WriteLog("Channel closed...")
		}
	}
}