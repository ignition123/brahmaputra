package ChannelList

import (
	"time"
	"log"
	"os"
	"sync"
)

var mutex = &sync.Mutex{}

func WriteLog(logMessage string){

	defer Recover()

	if !ConfigTCPObj.LogWrite{ 
		return
	}
	
	mutex.Lock()

	defer mutex.Unlock()

	ErrorFile, err := os.OpenFile("./storage/error.log", os.O_APPEND|os.O_WRONLY, 0600)

	if err != nil {
		log.Println(err)
		return
	}

	defer ErrorFile.Close()

	currentTime := time.Now()
	var logMsg = "############################### \r\n"
	logMsg += currentTime.String() + "\r\n"
	logMsg += logMessage + "\r\n"

	log.Println(logMsg)
	ErrorFile.WriteString(logMsg)
}
