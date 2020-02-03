package main

import (
	"fmt"
	"os"
	"io/ioutil"
	"pojo"
	"encoding/json"
	"server"
	"flag"
)


var commandLineMap = make(map[string]interface{})

func main(){

	commandLineargs := os.Args
	
	if len(commandLineargs) < 2{
		fmt.Println("No enough argument to start server")
		return
	}

	serverRun := flag.String("config", "default", "a string")

	channelName := flag.String("create-channel", "default", "a string")

	path := flag.String("path", "default", "a string")

	channelType := flag.String("channelType", "tcp", "a string")

	writeInterval := flag.Int("writeInterval", 1000, "a string")

	flag.Parse()

	if *serverRun != "default"{
		runConfigFile(*serverRun)
	}else if *channelName != "default"{
		createChannel(*path, *channelName, *channelType, *writeInterval)
	}else{
		fmt.Println(`
			possible commands:
			1) -config=d:\brahmaputra\config.json
			2) -create-channel=TestChannel -path=d:\brahmaputra\storage\
			3) -delete-channel=TestChannel -path=d:\brahmaputra\storage\
			4) -rename-channel=TestChannel -old-channel=TestChannel -new-channel=Test1Channel -path=d:\brahmaputra\storage\
			5) -host-panel=true -hosts=["192.168.90.22:7890","192.168.90.21:7890","192.168.90.20:7890"]
		`)
	}
}

func runConfigFile(configPath string){
	
	data, err := ioutil.ReadFile(configPath)

	if err != nil{
		fmt.Println("Failed to open config file from the path given")
		return
	}

	var configObj = pojo.Config{}

	pojoErr := json.Unmarshal(data, &configObj)

	if pojoErr != nil{
		fmt.Println("Invalid config file, json is not valid")
		return
	}

	if *configObj.Server.Host != "" && *configObj.Server.Port != ""{
		server.HostTCP(configObj)
	}
}

func createChannel(path string, channelName string, channelType string, writeInterval int){

	if path == "default"{
		fmt.Println("Please set a path for the channel storage...")
		return
	}

	var filePath = path+"/"+channelName+".br";

	if _, err := os.Stat(filePath); err == nil{

	  	fmt.Println("Channel already exists with name : "+channelName+"...")
		return

	}else if os.IsNotExist(err){

		if channelType != "tcp" && channelType != "udp"{
			fmt.Println("Channel must be either tcp or udp...")
			return
		}

		if writeInterval <= 1{
			fmt.Println("writeInterval must be greater than 0...")
			return
		}

		fDes, err := os.Create(filePath)

		if err != nil{

			fmt.Println(err)
			return

		}

		defer fDes.Close()
  	
		var bytePacket []byte

		server.Storage[channelName] = make(map[string]interface{})

		server.Storage[channelName]["path"] = path
		server.Storage[channelName]["bytePacket"] = bytePacket
		server.Storage[channelName]["writeInterval"] = writeInterval
		server.Storage[channelName]["channelType"] = channelType

		jsonData, err := json.Marshal(server.Storage[channelName])

		if err != nil{

			fmt.Println(err)
			return

		}

		d1 := []byte(jsonData)

		err = ioutil.WriteFile("./storage/"+channelName+"_channel_details.json", d1, 0644)

		if err != nil{

			fmt.Println(err)
			return

		}

		fmt.Println("Channel created successfully...")

	}else{
	  
		fmt.Println("Error")

	}

}