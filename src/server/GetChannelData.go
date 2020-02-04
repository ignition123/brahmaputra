package server

import(
	"fmt"
)

func GetChannelData(){

	go func(){
		for{
			fmt.Println("$$$$$$$$$$$$$$$$$")
			fmt.Println(<- TCPStorage["SampleChannel"].BucketData)
			fmt.Println("$$$$$$$$$$$$$$$$$")
		}
	}()

	go func(){
		for{
			fmt.Println("##################")
			fmt.Println(<- TCPStorage["Abhik"].BucketData)
			fmt.Println("##################")
		}
	}()
}