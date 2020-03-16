package main

import(
	"brahmaputra"
	"time"
	"fmt"
	_"sync"
	"log"
)

func main(){

	var brahm = &brahmaputra.CreateProperties{
		Host:"127.0.0.1",
		Port:"8100",
		AuthToken:"dkhashdkjshakhdksahkdghsagdghsakdsa",
		ConnectionType:"tcp",
		ChannelName:"Abhik",
		AppType:"producer",
	}

	brahm.Connect()

	var bodyMap = make(map[string]interface{})
			
	bodyMap["Account"] = "T93992"
	bodyMap["Exchange"] = "NSE"
	bodyMap["Segment"] = "CM"
	bodyMap["AlgoEndTime"] = 0
	bodyMap["AlgoSlices"] = 0
	bodyMap["AlgoSliceSeconds"] = 0 
	bodyMap["AlgoStartTime"] = 0
	bodyMap["ClientType"] = 2
	bodyMap["ClOrdID"] = "102173109118"
	bodyMap["ClTxnID"] = "D202002031731214230"
	bodyMap["ComplianceID"] = "1111111111111088"
	bodyMap["CoveredOrUncovered"] = 0
	// bodyMap["CreatedTime"] = currentTime.Unix()
	bodyMap["CustomerOrFirm"] = 0.0
	bodyMap["DisclosedQty"] = 0.0
	bodyMap["DripPrice"] = 0.0
	bodyMap["DripSize"] = 0.0
	bodyMap["Number"] = 10

	var bodyMap1 = make(map[string]interface{})
			
	bodyMap1["Account"] = "T93992"
	bodyMap1["Exchange"] = "NSE"
	bodyMap1["Segment"] = "FO"
	bodyMap1["AlgoEndTime"] = 0
	bodyMap1["AlgoSlices"] = 0
	bodyMap1["AlgoSliceSeconds"] = 0 
	bodyMap1["AlgoStartTime"] = 0
	bodyMap1["ClientType"] = 2
	bodyMap1["ClOrdID"] = "102173109118"
	bodyMap1["ClTxnID"] = "D202002031731214230"
	bodyMap1["ComplianceID"] = "1111111111111088"
	bodyMap1["CoveredOrUncovered"] = 0
	// bodyMap1["CreatedTime"] = currentTime.Unix()
	bodyMap1["CustomerOrFirm"] = 0.0
	bodyMap1["DisclosedQty"] = 0.0
	bodyMap1["DripPrice"] = 0.0
	bodyMap1["DripSize"] = 0.0
	bodyMap1["Number"] = 10

	go subscribe()

	// var parseWait sync.WaitGroup

	// var parseWait1 sync.WaitGroup

	for i:=0;i<1000000;i++{

		go brahm.Publish(bodyMap)

		time.Sleep(1 * time.Nanosecond)
	}

}

func subscribe(){

	fmt.Println("ok")

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

	go brahm.GetSubscribeMessage(msgChan)

	for{
		select{
			case chanCallback, ok := <-msgChan:	
				if ok{
					
					fmt.Println(chanCallback)
				}	
				break
			// default:
			// 	<-time.After(1 * time.Nanosecond)
			// 	break
		}
	}

}