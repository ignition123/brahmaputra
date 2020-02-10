package main

import (
	"fmt"
	"os"
	"io/ioutil"
	"encoding/json"
	"node_keeper"
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

	pojoErr := json.Unmarshal(data, &node_keeper.TCPClusters)

	if pojoErr != nil{
		fmt.Println(pojoErr)
		fmt.Println("Invalid config file, json is not valid")
		return
	}

	var tcpNode = node_keeper.TCPClusters["tcpHost"].(map [string]interface{})

	if tcpNode["active"].(bool){
		if tcpNode["host"] != "" && tcpNode["port"] != ""{
			defer keeper.HostTCPServer(tcpNode)
		}
	}

	go func(){

		var httpNode = node_keeper.TCPClusters["httpHost"].(map [string]interface{})

		if httpNode["active"].(bool){
			if httpNode["host"] != "" && httpNode["port"] != ""{
				defer keeper.HostHTTPServer(httpNode)
			}
		}

	}()
}
