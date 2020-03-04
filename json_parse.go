package main

import (
	"encoding/json"
	"fmt"
)

func main(){

	var cm = `
		{
			"$and":[
				{
					"$eq":{
						"Exchange":"CM"
					}
				},
				{
					"$eq":{
						"Segment":"FO"
					}
				}
			]
		}
	`

	var jsonObject = make(map[string]interface{})

	err := json.Unmarshal([]byte(cm), &jsonObject)

	if err != nil{
		fmt.Println(err)
		return
	}

	fmt.Println(jsonObject)
}

