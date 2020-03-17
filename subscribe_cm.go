/*
A very simple TCP client written in Go.
This is a toy project that I used to learn the fundamentals of writing
Go code and doing some really basic network stuff.
Maybe it will be fun for you to read. It's not meant to be
particularly idiomatic, or well-written for that matter.
*/
package main

import (
	"brahmaputra"
	"log"
)


func main() {
	
	var brahm = &brahmaputra.CreateProperties{
		Host:"127.0.0.1",
		Port:"8100",
		AuthToken:"dkhashdkjshakhdksahkdghsagdghsakdsa",
		ConnectionType:"tcp",
		ChannelName:"Abhik",
		AppType:"consumer",
	}

	brahm.Connect()

	var cm = `
		{
			"$eq":"all"
		}
	`

	brahm.Subscribe(cm)

	var msgChan = make(chan map[string]interface{})

	brahm.GetSubscribeMessage(msgChan)

	var count = 0

	for{
		select{
			case _, ok := <-msgChan:	
				if ok{
						
					count += 1 
						
					log.Println(count)

				}	

			break
		}
	}
}