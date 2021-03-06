package ChannelList

import(
	"encoding/json"
)

// converting objects, hashmap to json string

func JSONStringify(message map[string]interface{}, cb chan []byte){

	jsonData, err := json.Marshal(message)

	if err != nil{

		cb <- nil

	}else{

		cb <- jsonData

	}

}

// converting json string to hashmap

func JSONParse(packet []byte, message map[string]interface{}, cb chan map[string]interface{}){

	errJson := json.Unmarshal(packet, &message)

	if errJson != nil{
		
		cb <- nil

	}else{

		cb <- message

	}

}
