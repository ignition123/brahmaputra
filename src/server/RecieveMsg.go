package server


import(
	"net"
	"sync"
)

var mutex = &sync.Mutex{}

func RecieveMessage(conn net.Conn, messageQueue chan string){

	var stopIterate = false

	for{

		if stopIterate{
			break
		}

		var message = <-messageQueue

		if message == "BRAHMAPUTRA_DISCONNECT"{
			break
		}

		mutex.Lock()
		
		go ParseMsg(message, mutex, conn)
	}
}