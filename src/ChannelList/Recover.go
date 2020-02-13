package ChannelList

import(
	"runtime"
)

func Recover(){	
       if err := recover(); err != nil {
       WriteLog("RECOVER "+err.(string))
       runtime.Goexit()
   }	
}