/*
A very simple TCP client written in Go.
This is a toy project that I used to learn the fundamentals of writing
Go code and doing some really basic network stuff.
Maybe it will be fun for you to read. It's not meant to be
particularly idiomatic, or well-written for that matter.
*/
package main

import (
	_"bufio"
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"net"
	_ "regexp"
	"strconv"
	"strings"
	"time"
	"encoding/json"
	"sync"
)

var host = flag.String("host", "localhost", "The hostname or IP to connect to; defaults to \"localhost\".")
var port = flag.Int("port", 8100, "The port to connect to; defaults to 8000.")
var RequestMutex = &sync.Mutex{}

var counter = 0

func main() {
		
	createWorker()
}

func createWorker(){

	flag.Parse()

	dest := *host + ":" + strconv.Itoa(*port)
	fmt.Printf("Connecting to %s...\n", dest)

	conn, err := net.Dial("tcp", dest)

	if err != nil {
		if _, t := err.(*net.OpError); t {
			fmt.Println("Some problem connecting.")
		} else {
			fmt.Println("Unknown error: " + err.Error())
		}
		go createWorker()
		return
	}

	defer conn.Close()

	go readConnection(conn)

	for i:=0;i<1000000;i++{

		time.Sleep(10 * time.Millisecond)

		currentTime := time.Now()

		var _id = strconv.FormatInt(currentTime.UnixNano(), 10)

		//reader := bufio.NewReader(os.Stdin)

		text := `{ 
			"Exchange":"BSE", 
			"ExchangeSegment":"CM", 
			"ExecInst":0, 
			"Giveup":"0", 
			"GoodTillDate":0, 
			"HandlInst":1, 
			"InstruType":"E", 
			"IntiatedUserId":"T93992", 
			"IntiatedRequestMode":"D", 
			"LastModifiedTime":1580731269726, 
			"MastersLastUpdated":423, 
			"MaturityDay":0, 
			"MaxPricePercentage":0.000000e+00, 
			"MessageType":"D", 
			"MinimumFillAon":0.000000e+00, 
			"OMSID":"102_20200203173109_103", 
			"OptAttribute":"S", 
			"OrderQty":1.000000e+00, 
			"OrdStatus":"A", 
			"OrderRequestMode":"D", 
			"Ordmsgtyp":"1", 
			"OrdType":1, 
			"Price":0.000000e+00, 
			"PriceType":2, 
			"PrimaryDealer":"NA", 
			"PutOrCall":0, 
			"rSymbol":"523395_BSE_CM", 
			"ScripName":"3M INDIA LTD.", 
			"SecondaryDealer":"NA", 
			"Series":"A", 
			"Settlor":"0", 
			"Side":1, 
			"SpecialOrderFlag":4, 
			"Status":"Pending New", 
			"Symbol":"3MINDIA", 
			"TimeInForce":0, 
			"Token":"523395", 
			"TradingSession":1, 
			"TransacationType":0, 
			"TransactTime":1580731281423, 
			"UniqueSessionId":"eyJ0eXBlIjoiSldUIiwiYWxnIjoiSFMyNTYifQ==.eyJjbGllbnRDb2RlIjoiVDkzOTkyIiwic2Vzc2lvbklkIjoiNWUzODAzNDJlZmM2NWEwNzEzZWUwN2ZlIiwidXNlclR5cGUiOiJDdXN0b21lciIsImNyZWF0ZWRBdCI6IjIwMjAtMDItMDNUMTE6MjU6NTQuMTM5WiJ9.8fLjJG0/eXWLbVGD7otgMOsiGn6rCV8UmIsMkYUFenw"=", 
			"UserId":"T93992", 
			"PAN":"AAAAA1111A", 
			"ActivityTime":1580731269703, 
			"ClearingAccount":"8036"
		}`

		text = strings.TrimRight(text, "\r\n")

		// if text == "" {
		// 	fmt.Print("127.0.0.1:8100>")
		// 	continue
		// }


		var messageMap = make(map[string]interface{})
		messageMap["channelName"] = "Abhik"
		messageMap["type"] = "publish"
		messageMap["producer_id"] = _id

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
		bodyMap["CreatedTime"] = currentTime.Unix()
		bodyMap["CustomerOrFirm"] = 0.0
		bodyMap["DisclosedQty"] = 0.0
		bodyMap["DripPrice"] = 0.0
		bodyMap["DripSize"] = 0.0

		messageMap["data"] = bodyMap

		go sendMessage(messageMap, conn)

		//#############################################################

		var messageMap1 = make(map[string]interface{})
		messageMap1["channelName"] = "Abhik"
		messageMap1["type"] = "publish"
		messageMap1["producer_id"] = _id

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
		bodyMap1["CreatedTime"] = currentTime.Unix()
		bodyMap1["CustomerOrFirm"] = 0.0
		bodyMap1["DisclosedQty"] = 0.0
		bodyMap1["DripPrice"] = 0.0
		bodyMap1["DripSize"] = 0.0

		messageMap1["data"] = bodyMap1

		go sendMessage(messageMap1, conn)
		
	}
	// wg.Done()
}

func sendMessage(messageMap map[string]interface{}, conn net.Conn){

	RequestMutex.Lock()
	defer RequestMutex.Unlock()

	var packetBuffer bytes.Buffer

	buff := make([]byte, 4)

	jsonData, err := json.Marshal(messageMap)

	if err != nil{

		fmt.Println(err)
		return

	}

	binary.LittleEndian.PutUint32(buff, uint32(len(jsonData)))

	packetBuffer.Write(buff)

	packetBuffer.Write(jsonData)

	fmt.Println(string(jsonData))

	fmt.Println(time.Now())

	_, err = conn.Write(packetBuffer.Bytes())

	counter += 1

	fmt.Println(counter)
	// break

	// break

	if err != nil {
		fmt.Println("Error writing to stream." + err.Error())
	}
}

func allZero(s []byte) bool {
	for _, v := range s {
		if v != 0 {
			return false
		}
	}
	return true
}

func readConnection(conn net.Conn) {

	for {

		sizeBuf := make([]byte, 4)

		conn.Read(sizeBuf)

		packetSize := binary.LittleEndian.Uint32(sizeBuf)

		if packetSize < 0 {
			continue
		}

		completePacket := make([]byte, packetSize)

		conn.Read(completePacket)

		if allZero(completePacket) {
			fmt.Println("Server disconnected")
			break
		}

		var message = string(completePacket)

		fmt.Println(message)
	}

	conn.Close()
}
