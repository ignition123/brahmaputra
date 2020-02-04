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
	"os"
	_ "regexp"
	"strconv"
	"strings"
	"time"
	"encoding/json"
)

var host = flag.String("host", "localhost", "The hostname or IP to connect to; defaults to \"localhost\".")
var port = flag.Int("port", 8900, "The port to connect to; defaults to 8000.")

func main() {
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
		os.Exit(1)
	}

	//go readConnection(conn)

	fmt.Print("127.0.0.1:8900>")

	for {

		time.Sleep(10)

		currentTime := time.Now()

		//reader := bufio.NewReader(os.Stdin)

		text := `{string:Account="T93992", long:AlgoEndTime=0, long:AlgoSlices=0, long:AlgoSliceSeconds=0, long:AlgoStartTime=0, long:ClientType=2, string:ClOrdID="102173109118", string:ClTxnID="D202002031731214230", string:ComplianceID="1111111111111088", long:CoveredOrUncovered=0, long:CreatedTime=1580731269703, long:CustomerOrFirm=0, double:DisclosedQty=0.000000e+00, double:DripPrice=0.000000e+00, long:DripSize=0, string:Exchange="BSE", string:ExchangeSegment="CM", long:ExecInst=0, string:Giveup="0", long:GoodTillDate=0, long:HandlInst=1, string:InstruType="E", string:IntiatedUserId="T93992", string:IntiatedRequestMode="D", long:LastModifiedTime=1580731269726, long:MastersLastUpdated=423, long:MaturityDay=0, double:MaxPricePercentage=0.000000e+00, string:MessageType="D", double:MinimumFillAon=0.000000e+00, string:OMSID="102_20200203173109_103", string:OptAttribute="S", double:OrderQty=1.000000e+00, string:OrdStatus="A", string:OrderRequestMode="D", string:Ordmsgtyp="1", long:OrdType=1, double:Price=0.000000e+00, long:PriceType=2, string:PrimaryDealer="NA", long:PutOrCall=0, string:rSymbol="523395_BSE_CM", string:ScripName="3M INDIA LTD.", string:SecondaryDealer="NA", string:Series="A", string:Settlor="0", long:Side=1, long:SpecialOrderFlag=4, string:Status="Pending New", string:Symbol="3MINDIA", long:TimeInForce=0, string:Token="523395", long:TradingSession=1, long:TransacationType=0, long:TransactTime=1580731281423, string:UniqueSessionId="eyJ0eXBlIjoiSldUIiwiYWxnIjoiSFMyNTYifQ==.eyJjbGllbnRDb2RlIjoiVDkzOTkyIiwic2Vzc2lvbklkIjoiNWUzODAzNDJlZmM2NWEwNzEzZWUwN2ZlIiwidXNlclR5cGUiOiJDdXN0b21lciIsImNyZWF0ZWRBdCI6IjIwMjAtMDItMDNUMTE6MjU6NTQuMTM5WiJ9.8fLjJG0/eXWLbVGD7otgMOsiGn6rCV8UmIsMkYUFenw=", string:UserId="T93992", string:PAN="AAAAA1111A", long:ActivityTime=1580731269703, string:ClearingAccount="8036"}`

		text = strings.TrimRight(text, "\r\n")

		// if text == "" {
		// 	fmt.Print("127.0.0.1:8900>")
		// 	continue
		// }

		var packetBuffer bytes.Buffer

		buff := make([]byte, 4)

		var _id = strconv.FormatInt(currentTime.UnixNano(), 10)

		var messageMap = make(map[string]interface{})
		messageMap["_id"] = _id
		messageMap["channelName"] = "SampleChannel"
		messageMap["data"] = text

		jsonData, err := json.Marshal(messageMap)

		if err != nil{

			fmt.Println(err)
			return

		}

		binary.LittleEndian.PutUint32(buff, uint32(len(jsonData)))

		packetBuffer.Write(buff)

		packetBuffer.Write(jsonData)

		conn.SetWriteDeadline(time.Now().Add(1 * time.Second))

		fmt.Println(jsonData)

		fmt.Println(time.Now())
		_, err = conn.Write(packetBuffer.Bytes())

		// break

		if err != nil {
			fmt.Println("Error writing to stream." + err.Error())
		}

		//#############################################################

		messageMap = make(map[string]interface{})
		messageMap["_id"] = _id
		messageMap["channelName"] = "Abhik"
		messageMap["data"] = text

		jsonData, err = json.Marshal(messageMap)

		if err != nil{

			fmt.Println(err)
			return

		}

		binary.LittleEndian.PutUint32(buff, uint32(len(jsonData)))

		packetBuffer.Write(buff)

		packetBuffer.Write(jsonData)

		conn.SetWriteDeadline(time.Now().Add(1 * time.Second))

		fmt.Println(jsonData)

		fmt.Println(time.Now())
		_, err = conn.Write(packetBuffer.Bytes())

		// break

		if err != nil {
			fmt.Println("Error writing to stream." + err.Error())
		}
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

	sizeBuf := make([]byte, 4)

	statusBuf := make([]byte, 2)

	for {

		time.Sleep(1)

		conn.Read(sizeBuf)

		packetSize := binary.LittleEndian.Uint32(sizeBuf)

		if packetSize < 0 {
			continue
		}

		conn.Read(statusBuf)

		packetStatus := binary.LittleEndian.Uint16(statusBuf)

		if packetStatus != 1 && packetStatus != 2 {
			continue
		}

		completePacket := make([]byte, packetSize)

		conn.Read(completePacket)

		if allZero(completePacket) {
			fmt.Println("Server disconnected")
			os.Exit(1)
			break
		}

		var message = string(completePacket)

		if packetStatus == 1 {
			fmt.Println(message)
		} else {
			fmt.Println("Exception: " + message)
		}

		fmt.Print("127.0.0.1:8900>")
	}

	conn.Close()
}
