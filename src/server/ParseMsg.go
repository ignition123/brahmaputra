package server

import (
	_"pojo"
	"net"
	_"encoding/json"
	"fmt"
	"sync"
)

var mutex = &sync.Mutex{}

func ParseMsg(msg string, conn net.Conn){

	// if msg == "PING"{

	// 	WriteBytes(conn, 1, []byte("PONG"))

	// }else{

	// 	var QueryObject = pojo.QueryParser{}

	// 	pojoErr := json.Unmarshal([]byte(msg), &QueryObject)

	// 	if pojoErr != nil{
	// 		WriteBytes(conn, 2, []byte("Invalid message command..."))
	// 		return
	// 	}

	// 	// Cache Query SET, GET, DEL method to be invoked

	// 	if QueryObject.Ch != nil{
	// 		if *QueryObject.Ch.DB < 0 || *QueryObject.Ch.DB > 14{
	// 			WriteBytes(conn, 2, []byte("Invalid database selected, database lies between 0 - 14..."))
	// 			return
	// 		}
	// 		CallCacheMethod(conn, QueryObject)
	// 	}
	// }

	mutex.Lock()

	fmt.Println("################")
	fmt.Println(msg)

	File.WriteString(msg)

	mutex.Unlock()
}