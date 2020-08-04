package tcp

import(
	"ChannelList"
	"objects"
	"os"
	"io/ioutil"
	"encoding/binary"
)

// create a directory for subscriber group

func checkCreateGroupDirectory(channelName string, groupName string, clientObj *objects.ClientObject, checkDirectoryChan chan bool){

	defer ChannelList.Recover()

	// setting directory path

	directoryPath := objects.SubscriberObj[channelName].Channel.Path+"/"+groupName

	// getting the stat of the directory path

	if _, err := os.Stat(directoryPath); err == nil{

		checkDirectoryChan <- true

	}else if os.IsNotExist(err){ // checking if file exists

		errDir := os.MkdirAll(directoryPath, 0755)

		if errDir != nil { // if not equals to null then error
			
			ChannelList.ThroughGroupError(channelName, groupName, err.Error())

			objects.SubscriberObj[channelName].GroupUnRegister <- groupName

			checkDirectoryChan <- false

			return

		}

		// directory created successfully

		go ChannelList.WriteLog("Subscriber directory created successfully...")

		checkDirectoryChan <- true

	}else{

		checkDirectoryChan <- true

	}

}

// create subscriber group offset

func createSubscriberGroupOffsetFile(channelName string, groupName string, packetObject *objects.PacketStruct, start_from string, clientObj *objects.ClientObject, partitionOffsetSubscriber chan int64){

	defer ChannelList.Recover()

	// set directory path

	directoryPath := objects.SubscriberObj[channelName].Channel.Path+"/"+groupName

	// setting consumer offset path

	consumerOffsetPath := directoryPath+"\\"+groupName+"_offset_.index"

	// getting the os stat

	if _, err := os.Stat(consumerOffsetPath); err == nil{

		// getting the file descriptor object and setting to packetObject

		fDes, err := os.OpenFile(consumerOffsetPath,
			os.O_WRONLY, 0644) //race

		if err != nil {

			ChannelList.ThroughGroupError(channelName, groupName, err.Error())

			objects.SubscriberObj[channelName].GroupUnRegister <- groupName

			partitionOffsetSubscriber <- int64(-2)

			return
		}

		// adding to fD to packetObject

		packetObject.SubscriberFD = fDes // race

		// start_from == BEGINNING then offset = 0 means it will start reading file from beginning

		if start_from == "BEGINNING"{

			partitionOffsetSubscriber <- int64(0)

		}else if start_from == "NOPULL"{ // if start_from == NOPULL then offset = -1 then offset = file size

			partitionOffsetSubscriber <- int64(-1)

		}else if start_from == "LASTRECEIVED"{ // if start_from == LASTRECEIVED then offset = last offset written into the file

			// reading the file to get the last offset of the subscriber 

			dat, err := ioutil.ReadFile(consumerOffsetPath)

			if err != nil{

				ChannelList.ThroughGroupError(channelName, groupName, err.Error())

				objects.SubscriberObj[channelName].GroupUnRegister <- groupName

				partitionOffsetSubscriber <- int64(-2)

				return

			}

			// if data fetched from file === 0 then offset will be 0 else from file

			if len(dat) == 0{

				partitionOffsetSubscriber <- int64(0)

			}else{

				partitionOffsetSubscriber <- int64(binary.BigEndian.Uint64(dat))

			}

		}else{

			// offset = 0

			partitionOffsetSubscriber <- int64(0)

		}

	}else if os.IsNotExist(err){

		// if not exists then create a offset file

		fDes, err := os.Create(consumerOffsetPath)

		if err != nil{

			ChannelList.ThroughGroupError(channelName, groupName, err.Error())

			objects.SubscriberObj[channelName].GroupUnRegister <- groupName

			partitionOffsetSubscriber <- int64(-2)

			return

		}

		// then adding the file descriptor object

		packetObject.SubscriberFD = fDes

		if start_from == "BEGINNING"{

			partitionOffsetSubscriber <- int64(0)

		}else if start_from == "NOPULL"{

			partitionOffsetSubscriber <- int64(-1)

		}

	}else{

		// start_from == BEGINNING then offset = 0 means it will start reading file from beginning

		if start_from == "BEGINNING"{

			partitionOffsetSubscriber <- int64(0)

		}else if start_from == "NOPULL"{ // if start_from == NOPULL then offset = -1 then offset = file size

			partitionOffsetSubscriber <- int64(-1)

		}else if start_from == "LASTRECEIVED"{ // if start_from == LASTRECEIVED then offset = last offset written into the file

			// reading the file to get the last offset of the subscriber 

			dat, err := ioutil.ReadFile(consumerOffsetPath)

			if err != nil{

				ChannelList.ThroughGroupError(channelName, groupName, err.Error())

				objects.SubscriberObj[channelName].GroupUnRegister <- groupName

				partitionOffsetSubscriber <- int64(-2)

				return

			}	

			// if data fetched from file === 0 then offset will be 0 else from file

			if len(dat) == 0{

				partitionOffsetSubscriber <- int64(0)

			}else{

				// read the file bytes

				partitionOffsetSubscriber <- int64(binary.BigEndian.Uint64(dat))

			}

		}else{

			// offset = 0 reading from beginning of the file

			partitionOffsetSubscriber <- int64(0)

		}

	}

}


