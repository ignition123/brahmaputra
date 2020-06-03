package ChannelList

// method to handle panics

func Handlepanic() { 
  
    if a := recover(); a != nil { 
        WriteLog("RECOVER "+a.(string))
    } 
} 