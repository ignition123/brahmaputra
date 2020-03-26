package main

import (
	"brahmaputra"
	"log"
	"time"
)


func main() {
	
	var brahm = &brahmaputra.CreateProperties{
		Url:"brahm://127.0.0.1:8100",
		AuthToken:"dkhashdkjshakhdksahkdghsagdghsakdsa",
		ConnectionType:"tcp",
		ChannelName:"brahm",
		AppType:"consumer",
		AlwaysStartFrom:"BEGINNING", // BEGINNING | NOPULL | LASTRECEIVED,
		ReadDelay:0, // nano second
		SubscriberName:"sudeep_subscriber",
		GroupName:"brahm_group",
		Worker:1,
	}

	brahm.Connect()

	var cm = `
		{
			"$eq":"all"
		}
	`

	brahm.Subscribe(cm)

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