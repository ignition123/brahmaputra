package server

import(
	"fmt"
)

func GetChannelData(){

	go func(){
		for{
			var channelMess = TCPStorage["SampleChannel"]["bucketData"].(chan string)
			fmt.Println("$$$$$$$$$$$$$$$$$")
			fmt.Println(<- channelMess)
			fmt.Println("$$$$$$$$$$$$$$$$$")
		}
	}()
}