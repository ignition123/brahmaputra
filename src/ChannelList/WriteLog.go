package ChannelList

import (
	"time"
	"fmt"
	"os"
	"sync"
)

var mutex = &sync.Mutex{}

func WriteLog(logMessage string){

	defer Recover()
	
	mutex.Lock()

	defer mutex.Unlock()

	ErrorFile, err := os.OpenFile("./storage/error.log", os.O_APPEND|os.O_WRONLY, 0600)

	if err != nil {
		fmt.Println(err)
		return
	}

	defer ErrorFile.Close()

	currentTime := time.Now()
	var logMsg = "############################### \r\n"
	logMsg += currentTime.String() + "\r\n"
	logMsg += logMessage + "\r\n"

	fmt.Println(logMsg)
	ErrorFile.WriteString(logMsg)
}