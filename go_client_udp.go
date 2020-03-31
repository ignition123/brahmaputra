package main

import(
	"brahmaputra"
	"time"
	_"fmt"
	_"sync"
	_"runtime"
	"log"
	"encoding/json"
)

func main(){

	var brahm = &brahmaputra.CreateProperties{
		Url:"brahm://127.0.0.1:8100",
		AuthToken:"dkhashdkjshakhdksahkdghsagdghsakdsa",
		ConnectionType:"udp",
		ChannelName:"brahm",
		AppType:"producer",
		Worker:1, //runtime.NumCPU() runtime.NumCPU()
		PoolSize:10,
		WriteDelay:0, // nano second
		Acknowledge:true,
	}

	brahm.Connect()

}