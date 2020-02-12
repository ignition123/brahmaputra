package ChannelList

func Handlepanic() { 
  
    if a := recover(); a != nil { 
        WriteLog("RECOVER "+a.(string))
    } 
} 