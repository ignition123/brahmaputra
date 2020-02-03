package server

import (
	"time"
	"fmt"
	"os"
)

func WriteLog(logMessage string){

	ErrorFile, err := os.OpenFile("./storage/error.log", os.O_APPEND|os.O_WRONLY, 0600)

	if err != nil {
		fmt.Println(err)
		return
	}

	defer ErrorFile.Close()

	currentTime := time.Now()
	var logMsg = "############################### \r\n"
	logMsg += currentTime.String() + "\r\n"
	logMsg += logMsg

	ErrorFile.WriteString(logMsg)
}