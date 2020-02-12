package server


import(
	"net"
	"ChannelList"
	"strconv"
	"time"
)

func RecieveMessage(conn net.Conn, messageQueue chan string){

	defer ChannelList.Handlepanic()
	
	var stopIterate = false

	TCPTotalConnection += 1

	ChannelList.WriteLog("Total connection now open: "+strconv.Itoa(TCPTotalConnection))

	for{

		if stopIterate{
			break
		}

		time.Sleep(time.Millisecond)

		select {
			case val, ok := <-messageQueue:
				if ok{

					if val == "BRAHMAPUTRA_DISCONNECT"{
						break
					}

					ParseMsg(val, conn)

				}else{
					ChannelList.WriteLog("Connection closed!")
					ChannelList.WriteLog("Channel closed!")
					TCPTotalConnection -= 1
					ChannelList.WriteLog("Total connection now open: "+strconv.Itoa(TCPTotalConnection))
					stopIterate = true
					break
				
				}
		}
	}
}