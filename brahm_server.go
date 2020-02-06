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
		runConfigFile(*serverRun, *channelType)
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

func runConfigFile(configPath string, channelType string){
	
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

	if channelType == "tcp"{
		if *configObj.Server.TCP.Host != "" && *configObj.Server.TCP.Port != ""{
			server.HostTCP(configObj)
		}
	}else if channelType == "udp"{
		if *configObj.Server.UDP.Host != "" && *configObj.Server.UDP.Port != ""{
			server.HostUDP(configObj)
		}
	}else{
		fmt.Println("Invalid protocol, must be either tcp or udp...")
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

		var storage = make(map[string]map[string]interface{})

		if channelType == "tcp"{
			storage[channelName] = make(map[string]interface{})

			storage[channelName]["channelName"] = channelName
			storage[channelName]["type"] = "channel"
			storage[channelName]["path"] = path
			storage[channelName]["writeInterval"] = writeInterval
			storage[channelName]["channelType"] = channelType
		}else if channelType == "udp"{
			storage[channelName] = make(map[string]interface{})

			storage[channelName]["channelName"] = channelName
			storage[channelName]["type"] = "channel"
			storage[channelName]["path"] = path
			storage[channelName]["writeInterval"] = writeInterval
			storage[channelName]["channelType"] = channelType
		}else{
			fmt.Println("Invalid protocol, must be either tcp or udp...")
			return
		}

		jsonData, err := json.Marshal(storage[channelName])

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