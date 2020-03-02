package ChannelList

import(
	"runtime"
	"fmt"
)

func Recover(){	
       if err := recover(); err != nil {

       	fmt.Println(err)

		runtime.Goexit()
   }	
}