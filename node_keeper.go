package main

import (
	"fmt"
	"os"
	"io/ioutil"
	"pojo"
	"encoding/json"
	"keeper"
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

	flag.Parse()

	if *serverRun != "default"{
		runConfigFile(*serverRun)
	}
}

func runConfigFile(configPath string){
	
	data, err := ioutil.ReadFile(configPath)

	if err != nil{
		fmt.Println("Failed to open config file from the path given")
		return
	}

	var configObj = pojo.KeeperStruct{}

	pojoErr := json.Unmarshal(data, &configObj)

	if pojoErr != nil{
		fmt.Println("Invalid config file, json is not valid")
		return
	}

	if *configObj.Host != "" && *configObj.Port != ""{
		keeper.HostServer(configObj)
	}
}
