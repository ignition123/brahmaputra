package main

import (
	"log"
	"os"
	"io/ioutil"
	"pojo"
	"encoding/json"
	"server"
	"flag"
	"syscall"
	"os/signal"
	"ChannelList"
	"time"
	"runtime"
	"strconv"
)


var commandLineMap = make(map[string]interface{})

func main(){

	defer ChannelList.Recover()

	sigs := make(chan os.Signal, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL, syscall.SIGQUIT)

	go func() {

	  <- sigs
	  log.Println("Closing process...")
	  cleanupAllTheThings()
	  os.Exit(0)

	}()

	log.Println("Starting server logs...")

	// go server.ShowUtilization()

	commandLineargs := os.Args
	
	if len(commandLineargs) < 2{
		log.Println("No enough argument to start server")
		return
	}

	serverRun := flag.String("config", "default", "a string")

	channelName := flag.String("create-channel", "default", "a string")

	path := flag.String("path", "default", "a string")

	channelType := flag.String("channelType", "tcp", "a string")

	partionCount := flag.Int("partionCount", 5, "an int")
	
	flag.Parse()

	if *serverRun != "default"{
		runConfigFile(*serverRun, *channelType)
	}else if *channelName != "default"{
		createChannel(*path, *channelName, *channelType, *partionCount)
	}else{
		log.Println(`
			possible commands:
			1) -config=d:\brahmaputra\config.json
			2) -create-channel=TestChannel -path=d:\brahmaputra\storage\
			3) -delete-channel=TestChannel -path=d:\brahmaputra\storage\
			4) -rename-channel=TestChannel -old-channel=TestChannel -new-channel=Test1Channel -path=d:\brahmaputra\storage\
			5) -reclaim-drive=true -path=d:\brahmaputra\storage\
			6)
		`)
	}
}

func cleanupAllTheThings(){

	defer ChannelList.Recover()

	for key := range ChannelList.TCPStorage{

		time.Sleep(1 * time.Second)

		log.Println("Closing storage files of the channel "+ key+"...")

		for index :=  range ChannelList.TCPStorage[key].FD{

			ChannelList.TCPStorage[key].FD[index].Close()

		}
		
	}
}

func runConfigFile(configPath string, channelType string){
	
	defer ChannelList.Recover()

	data, err := ioutil.ReadFile(configPath)

	if err != nil{
		log.Println("Failed to open config file from the path given")
		return
	}

	var configObj = pojo.Config{}

	pojoErr := json.Unmarshal(data, &configObj)

	if pojoErr != nil{
		log.Println("Invalid config file, json is not valid")
		return
	}

	runtime.GOMAXPROCS(*configObj.Worker)

	if channelType == "tcp"{
		if *configObj.Server.TCP.Host != "" && *configObj.Server.TCP.Port != ""{
			server.HostTCP(configObj)
		}
	}else if channelType == "udp"{
		if *configObj.Server.UDP.Host != "" && *configObj.Server.UDP.Port != ""{
			server.HostUDP(configObj)
		}
	}else{
		log.Println("Invalid protocol, must be either tcp or udp...")
	}	
}

func createChannel(path string, channelName string, channelType string, partionCount int){

	defer ChannelList.Recover()

	if path == "default"{
		log.Println("Please set a path for the channel storage...")
		return
	}

	var directoryPath = path+"\\"+channelName

	if _, err := os.Stat(directoryPath); err == nil{

		log.Println("Channel already exists with name : "+channelName+"...")
		return

	}else if os.IsNotExist(err){

		errDir := os.MkdirAll(directoryPath, 0755)

		if errDir != nil {
				
			log.Println("Failed to create channel directory : "+channelName+"...")
			return

		}


		log.Println("Channel directory : "+channelName+" created successfully...")
	}else{
		  
		log.Println("Error")

	}

	for i:=0;i<partionCount;i++{

		var filePath = directoryPath+"\\"+channelName+"_partition_"+strconv.Itoa(i)+".br"

		if _, err := os.Stat(filePath); err == nil{

		  	log.Println("Failed to create partion name : "+channelName+"...")

			return

		}else if os.IsNotExist(err){

			if channelType != "tcp" && channelType != "udp"{
				log.Println("Channel must be either tcp or udp...")
				return
			}

			fDes, err := os.Create(filePath)

			if err != nil{

				log.Println(err)

				return

			}

			defer fDes.Close()

		}else{
		  
			log.Println("Error")

		}

	}

	
	var storage = make(map[string]map[string]interface{})

	if channelType == "tcp"{
		
		storage[channelName] = make(map[string]interface{})

		storage[channelName]["channelName"] = channelName
		storage[channelName]["type"] = "channel"
		storage[channelName]["channelStorageType"] = "persistent"
		storage[channelName]["path"] = directoryPath
		storage[channelName]["worker"] = 1
		storage[channelName]["channelType"] = channelType
		storage[channelName]["partitions"] = partionCount

	}else if channelType == "udp"{

		storage[channelName] = make(map[string]interface{})

		storage[channelName]["channelName"] = channelName
		storage[channelName]["type"] = "channel"
		storage[channelName]["channelStorageType"] = "inmemory"
		storage[channelName]["path"] = directoryPath
		storage[channelName]["worker"] = 1
		storage[channelName]["channelType"] = channelType

	}else{

		log.Println("Invalid protocol, must be either tcp or udp...")
		return

	}

	jsonData, err := json.Marshal(storage[channelName])

	if err != nil{

		log.Println(err)
		return

	}

	d1 := []byte(jsonData)

	err = ioutil.WriteFile(directoryPath+"\\"+channelName+"_channel_details.json", d1, 0644)

	if err != nil{

		log.Println(err)
		return

	}

	log.Println("Channel created successfully...")
	
}