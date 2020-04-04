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
	"fmt"
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

	// go server.ShowUtilization()

	commandLineargs := os.Args
	
	if len(commandLineargs) < 2{
		log.Println("No enough argument to start server")
		return
	}

	serverRun := flag.String("config", "default", "a string")

	channelName := flag.String("create-channel", "default", "a string")

	path := flag.String("path", "default", "a string")

	channelType := flag.String("channeltype", "tcp", "a string")

	partionCount := flag.Int("partionCount", 5, "an int")

	authUrl := flag.String("authUrl", "", "a string")

	// rtmp flags

	hls_fragment := flag.Int("hls_fragment", 5, "an int")

	hls_window := flag.Int("hls_window", 2, "an int")

	hls_path := flag.String("hls_path", "default", "a string")

	// on stream start events, both will get killed on stream end

	onPublish_authUrl := flag.String("onPublish_authUrl", "default", "a string")

	onPublish_hookCall := flag.String("onPublish_hookCall", "default", "a string")

	onPublish_exec := flag.String("onPublish_exec", "default", "a string")

	// on play events, both will get killed on stream end

	onPlay_authUrl := flag.String("onPlay_authUrl", "default", "a string")

	onPlay_hookCall := flag.String("onPlay_hookCall", "default", "a string")

	onPlay_exec := flag.String("onPlay_exec", "default", "a string")

	// on stream end events, dont call any such process which have to be stopped manually as after this event it wont close process

	onEnd_hookCall := flag.String("onEnd_hookCall", "default", "a string")

	onEnd_exec := flag.String("onEnd_exec", "default", "a string")

	flag.Parse()

	if *serverRun != "default"{
		runConfigFile(*serverRun)
	}else if *channelName != "default"{

		if *channelType == "tcp"{

			createTCPChannel(*path, *channelName, *channelType, *partionCount, *authUrl)

		}else if *channelType == "udp"{

			createUDPChannel(*path, *channelName, *channelType, *partionCount, *authUrl)

		}else if *channelType == "rtmp"{

			createRTMPChannel(*path, *channelName, *channelType, *onPublish_authUrl, *onPublish_hookCall, *onPublish_exec, *onPlay_authUrl, *onPlay_hookCall, *onPlay_exec, *onEnd_hookCall, *onEnd_exec, *hls_fragment, *hls_window, *hls_path)

		}else{

			log.Println("Invalid channelType, please refer the documentation.")

		}

		
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

	for key := range ChannelList.UDPStorage{

		time.Sleep(1 * time.Second)

		log.Println("Closing storage files of the channel "+ key+"...")

		for index :=  range ChannelList.UDPStorage[key].FD{

			ChannelList.UDPStorage[key].FD[index].Close()

		}
		
	}
}

func runConfigFile(configPath string){
	
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

	printLogo()

	log.Println("Starting server logs...")

	if *configObj.Server.TCP.Host != "" && *configObj.Server.TCP.Port != ""{
		go func(){

			time.Sleep(1 * time.Second)

			server.HostTCP(configObj)

		}()
	}

	if *configObj.Server.UDP.Host != "" && *configObj.Server.UDP.Port != ""{
		go func(){

			time.Sleep(1 * time.Second)
			
			server.HostUDP(configObj)

		}()
	}

	if *configObj.Server.RTMP.Host != "" && *configObj.Server.RTMP.Port != ""{
		go func(){

			time.Sleep(1 * time.Second)
			
			server.HostRTMP(configObj)

		}()
	}

	for{

		log.Println("Brahmaputra server started...")
		time.Sleep(1 * time.Hour)

	}
}

func printLogo(){

	year, _, _ := time.Now().Date()

	fmt.Println(`Open Source Project, by 79 Labs `+strconv.Itoa(year))

	fmt.Println(`Support this community to create best enterprise level applications`)

	var LOGO = `
		*****  ***** ***** *   * *** *** ***** ***** *   * ***** ***** *****
		*   *  *   * *   * *   * * * * * *   * *   * *   *   *   *   * *   *
		*   *  *   * *   * *   * * * * * *   * *   * *   *   *   *   * *   *
		*****  ***** ***** ***** * *** * ***** ***** *   *   *   ***** *****  
		*   *  *     *   * *   * *     * *   * *     *   *   *   *     *   *
		*   *  * *   *   * *   * *     * *   * *     *   *   *   * *   *   *
		*****  *   * *   * *   * *     * *   * *     *****   *   *   * *   *
	`

	fmt.Println(LOGO)  
}

func createUDPChannel(path string, channelName string, channelType string, partionCount int, authUrl string){

	defer ChannelList.Recover()

	if path == "default"{
		log.Println("Please set a path for the channel storage...")
		return
	}

	if partionCount <= 0{
		log.Println("Patition count must be greater than 0...")
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

	storage[channelName] = make(map[string]interface{})

	storage[channelName]["channelName"] = channelName
	storage[channelName]["type"] = "channel"
	storage[channelName]["channelStorageType"] = "persistent"
	storage[channelName]["path"] = directoryPath
	storage[channelName]["channelType"] = channelType
	storage[channelName]["partitions"] = partionCount
	storage[channelName]["authUrl"] = authUrl

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

	log.Println("UDP channel created successfully...")

}

func createTCPChannel(path string, channelName string, channelType string, partionCount int, authUrl string){

	defer ChannelList.Recover()

	if path == "default"{
		log.Println("Please set a path for the channel storage...")
		return
	}

	if partionCount <= 0{
		log.Println("Patition count must be greater than 0...")
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

	storage[channelName] = make(map[string]interface{})

	storage[channelName]["channelName"] = channelName
	storage[channelName]["type"] = "channel"
	storage[channelName]["channelStorageType"] = "persistent"
	storage[channelName]["path"] = directoryPath
	storage[channelName]["worker"] = 1
	storage[channelName]["channelType"] = channelType
	storage[channelName]["partitions"] = partionCount
	storage[channelName]["authUrl"] = authUrl

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

	log.Println("TCP channel created successfully...")
	
}

func createRTMPChannel(path string, channelName string, channelType string, onPublish_authUrl string, onPublish_hookCall string, onPublish_exec string, onPlay_authUrl string, onPlay_hookCall string, onPlay_exec string, onEnd_hookCall string, onEnd_exec string, hls_fragment int, hls_window int, hls_path string){

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

	// creating hashmap of onPublish events of RTMP

	var onPublish = make(map[string]interface{})

	if onPublish_authUrl != "default"{

		onPublish["authUrl"] = onPublish_authUrl

	}

	if onPublish_hookCall != "default"{

		onPublish["hookCall"] = onPublish_hookCall

	}

	if onPublish_exec != "default"{

		onPublish["exec"] = onPublish_exec

	}

	// creating hashmap of onPlay events of RTMP

	var onPlay = make(map[string]interface{})

	if onPlay_authUrl != "default"{

		onPlay["authUrl"] = onPlay_authUrl

	}

	if onPlay_hookCall != "default"{

		onPlay["hookCall"] = onPlay_hookCall

	}

	if onPlay_exec != "default"{

		onPlay["exec"] = onPlay_exec

	}

	// creating hashmap for onEnd event

	var onEnd = make(map[string]interface{})

	if onEnd_hookCall != "default"{

		onEnd["hookCall"] = onEnd_hookCall

	}

	if onEnd_exec != "default"{

		onEnd["exec"] = onEnd_exec

	}

	// create hashmap for hls configuration

	var hls = make(map[string]interface{})

	if hls_path != "default"{

		if _, err := os.Stat(hls_path); err != nil{

			errDir := os.MkdirAll(hls_path, 0755)

			if errDir != nil {
					
				log.Println("Failed to create channel directory : "+channelName+"...")
				return

			}

		}

		hls["hls_path"] = hls_path

		hls["hls_fragment"] = hls_fragment

		hls["hls_window"] = hls_window

	}

	var storage = make(map[string]map[string]interface{})

	storage[channelName] = make(map[string]interface{})

	storage[channelName]["channelName"] = channelName
	storage[channelName]["type"] = "channel"
	storage[channelName]["channelType"] = channelType
	storage[channelName]["onPublish"] = onPublish
	storage[channelName]["onPlay"] = onPlay
	storage[channelName]["onEnd"] = onEnd
	storage[channelName]["hls"] = hls

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

	log.Println("RTMP channel created successfully...")
}