package ChannelList

import(
	"log"
)

// handling error and panics

func Recover(){	
    if err := recover(); err != nil {

       	log.Println(err)
   }	
}