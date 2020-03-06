package main

import(
	"brahmaputra"
	"time"
	_"fmt"
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

	for i:=0;i<100;i++{

		go brahm.Publish(bodyMap)

		time.Sleep(1 * time.Nanosecond)

	}

}