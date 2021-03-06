package main

import (
	"brahmaputra"
	"log"
	"time"
	// "syscall"
	// "os/signal"
	// "os"
)


func main() {
	
	var brahm = &brahmaputra.CreateProperties{
		Url:"brahm://127.0.0.1:8100",
		AuthToken:"dkhashdkjshakhdksahkdghsagdghsakdsa",
		ConnectionType:"tcp",
		ChannelName:"brahm",
		AppType:"consumer",
		AlwaysStartFrom:"LASTRECEIVED", // BEGINNING | NOPULL | LASTRECEIVED,
		ReadDelay:0, // nano second
		SubscriberName:"sudeep_subscriber_fo1",
		GroupName:"brahm_group", //brahm_group_123
		Worker:1,
		AuthReconnect:false,
		Polling:100000,
		AutoCommit:true,
	}

	// sigs := make(chan os.Signal, 1)

	// signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL, syscall.SIGQUIT)

	// go func() {

	//   <- sigs

	//   brahm.Close()

	//   log.Println("Closing process...")

	//   time.Sleep(5 * time.Second)

	//   os.Exit(0)

	// }()

	brahm.Connect()

	// var cm = `
	// 	{
	// 		"$eq":"all"
	// 	}
	// `

	brahm.Subscribe("")

	time.Sleep(2 * time.Second)

	var count = 0

	for{
		select{
			case _, ok := <-brahmaputra.SubscriberChannel:	
				if ok{
						
					count += 1 
						
					log.Println(count)

				}	

			break
		}
	}
}