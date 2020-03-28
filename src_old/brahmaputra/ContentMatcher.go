package brahmaputra

import(
	"reflect"
	"math"
)

const TOLERANCE = 0.00000000001

func AndMatch(messageData map[string]interface{}, cm map[string]interface{}) bool{

	var contentMatch = cm["$and"].([]interface {})

	if len(contentMatch) > 0{

		for index := range contentMatch{

			var innerContentMatch = contentMatch[index].(map[string]interface{})

			// return false if content matcher does not match to the data sent by publisher

			if _, found := innerContentMatch["$eq"]; found {
			    
			   	var equalMap = innerContentMatch["$eq"].(map[string]interface{})

			   	for key := range equalMap{

			   		var datatype = reflect.TypeOf(equalMap[key])

					var datatype_msg = reflect.TypeOf(messageData[key])

					if datatype.Kind() == reflect.Float64 && datatype_msg.Kind() == reflect.Float64{

						if diff := math.Abs(equalMap[key].(float64) - messageData[key].(float64)); diff < TOLERANCE{

							return true

						}else{

							return false

						}

					}else{

						if equalMap[key] != messageData[key]{
						
							return false

						}

					}

			   	}

			}

			// return false if content matcher matches to the data sent by publisher

			if _, found := innerContentMatch["$ne"]; found {
			    
			   	var notequalMap = innerContentMatch["$ne"].(map[string]interface{})

			   	for key := range notequalMap{

			   		var datatype = reflect.TypeOf(notequalMap[key])

					var datatype_msg = reflect.TypeOf(messageData[key])

					if datatype.Kind() == reflect.Float64 && datatype_msg.Kind() == reflect.Float64{

						if diff := math.Abs(notequalMap[key].(float64) - messageData[key].(float64)); diff < TOLERANCE{

							return false

						}

					}else{

						if notequalMap[key] == messageData[key]{
						
							return false

						}

					}

			   	}

			}

			// return false if content matcher is not greater than data sent by publisher


			if _, found := innerContentMatch["$gt"]; found {
			    
			   	var greaterThanMap = innerContentMatch["$gt"].(map[string]interface{})

			   	for key := range greaterThanMap{

					var datatype = reflect.TypeOf(greaterThanMap[key])

					var datatype_msg = reflect.TypeOf(messageData[key])

					if datatype.Kind() == reflect.Float64 && datatype_msg.Kind() == reflect.Float64{

						if diff := math.Abs(greaterThanMap[key].(float64) - messageData[key].(float64)); diff < TOLERANCE{

							return false

						}

						if greaterThanMap[key].(float64) > messageData[key].(float64){

							return false

						}

					}

			   	}

			}

			// return false if content matcher is not greater equals than data sent by publisher

			if _, found := innerContentMatch["$gte"]; found {
			    
			   	var greaterThanEqualMap = innerContentMatch["$gte"].(map[string]interface{})

			   	for key := range greaterThanEqualMap{

					var datatype = reflect.TypeOf(greaterThanEqualMap[key])

					var datatype_msg = reflect.TypeOf(messageData[key])

					if datatype.Kind() == reflect.Float64 && datatype_msg.Kind() == reflect.Float64{

						if diff := math.Abs(greaterThanEqualMap[key].(float64) - messageData[key].(float64)); diff < TOLERANCE{

							return true

						}

						if greaterThanEqualMap[key].(float64) >= messageData[key].(float64){
						
							return false

						}

					}

			   	}

			}

			// return false if content matcher is not less than data sent by publisher

			if _, found := innerContentMatch["$lt"]; found {
			    
			   	var lessThanMap = innerContentMatch["$lt"].(map[string]interface{})

			   	for key := range lessThanMap{

					var datatype = reflect.TypeOf(lessThanMap[key])

					var datatype_msg = reflect.TypeOf(messageData[key])

					if datatype.Kind() == reflect.Float64 && datatype_msg.Kind() == reflect.Float64{

						if diff := math.Abs(lessThanMap[key].(float64) - messageData[key].(float64)); diff < TOLERANCE{

							return false

						}

						if lessThanMap[key].(float64) < messageData[key].(float64){

							return false

						}

					}

			   	}

			}

			// return false if content matcher is not less equals than data sent by publisher

			if _, found := innerContentMatch["$lte"]; found {
			    
			   	var lessThanEqualMap = innerContentMatch["$lte"].(map[string]interface{})

			   	for key := range lessThanEqualMap{

					var datatype = reflect.TypeOf(lessThanEqualMap[key])

					var datatype_msg = reflect.TypeOf(messageData[key])

					if datatype.Kind() == reflect.Float64 && datatype_msg.Kind() == reflect.Float64{

						if diff := math.Abs(lessThanEqualMap[key].(float64) - messageData[key].(float64)); diff < TOLERANCE{

							return true

						}

						if lessThanEqualMap[key].(float64) <= messageData[key].(float64){
						
							return false

						}

					}

			   	}

			}

		}

		return true

	}else{

		return false

	}

}

func OrMatch(messageData map[string]interface{}, cm map[string]interface{}) bool{

	var contentMatch = cm["$or"].([]interface {})

	var contentMatchLen = len(contentMatch)

	if contentMatchLen > 0{

		var totalCount = 0

		for index := range contentMatch{

			var innerContentMatch = contentMatch[index].(map[string]interface{})

			// return false if content matcher does not match to the data sent by publisher

			if _, found := innerContentMatch["$eq"]; found {
			    
			   	var equalMap = innerContentMatch["$eq"].(map[string]interface{})

			   	for key := range equalMap{

			   		var datatype = reflect.TypeOf(equalMap[key])

					var datatype_msg = reflect.TypeOf(messageData[key])

					if datatype.Kind() == reflect.Float64 && datatype_msg.Kind() == reflect.Float64{

						if diff := math.Abs(equalMap[key].(float64) - messageData[key].(float64)); diff < TOLERANCE{

							return true

						}else{

							totalCount += 1

						}

					}else{

						if equalMap[key] != messageData[key]{
						
							totalCount += 1

						}

					}

			   	}

			}

			// return false if content matcher matches to the data sent by publisher

			if _, found := innerContentMatch["$ne"]; found {
			    
			   	var notequalMap = innerContentMatch["$ne"].(map[string]interface{})

			   	for key := range notequalMap{

			   		var datatype = reflect.TypeOf(notequalMap[key])

					var datatype_msg = reflect.TypeOf(messageData[key])

					if datatype.Kind() == reflect.Float64 && datatype_msg.Kind() == reflect.Float64{

						if diff := math.Abs(notequalMap[key].(float64) - messageData[key].(float64)); diff < TOLERANCE{

							totalCount += 1

						}

					}else{

						if notequalMap[key] == messageData[key]{
						
							totalCount += 1

						}

					}

			   	}

			}

			// return false if content matcher is not greater than data sent by publisher


			if _, found := innerContentMatch["$gt"]; found {
			    
			   	var greaterThanMap = innerContentMatch["$gt"].(map[string]interface{})

			   	for key := range greaterThanMap{

					var datatype = reflect.TypeOf(greaterThanMap[key])

					var datatype_msg = reflect.TypeOf(messageData[key])

					if datatype.Kind() == reflect.Float64 && datatype_msg.Kind() == reflect.Float64{

						if diff := math.Abs(greaterThanMap[key].(float64) - messageData[key].(float64)); diff < TOLERANCE{

							totalCount += 1

						}

						if greaterThanMap[key].(float64) > messageData[key].(float64){

							totalCount += 1

						}

					}

			   	}

			}

			// return false if content matcher is not greater equals than data sent by publisher

			if _, found := innerContentMatch["$gte"]; found {
			    
			   	var greaterThanEqualMap = innerContentMatch["$gte"].(map[string]interface{})

			   	for key := range greaterThanEqualMap{

					var datatype = reflect.TypeOf(greaterThanEqualMap[key])

					var datatype_msg = reflect.TypeOf(messageData[key])

					if datatype.Kind() == reflect.Float64 && datatype_msg.Kind() == reflect.Float64{

						if diff := math.Abs(greaterThanEqualMap[key].(float64) - messageData[key].(float64)); diff < TOLERANCE{

							return true

						}

						if greaterThanEqualMap[key].(float64) >= messageData[key].(float64){
						
							totalCount += 1

						}

					}

			   	}

			}

			// return false if content matcher is not less than data sent by publisher

			if _, found := innerContentMatch["$lt"]; found {
			    
			   	var lessThanMap = innerContentMatch["$lt"].(map[string]interface{})

			   	for key := range lessThanMap{

					var datatype = reflect.TypeOf(lessThanMap[key])

					var datatype_msg = reflect.TypeOf(messageData[key])

					if datatype.Kind() == reflect.Float64 && datatype_msg.Kind() == reflect.Float64{

						if diff := math.Abs(lessThanMap[key].(float64) - messageData[key].(float64)); diff < TOLERANCE{

							totalCount += 1

						}

						if lessThanMap[key].(float64) < messageData[key].(float64){

							totalCount += 1

						}

					}

			   	}

			}

			// return false if content matcher is not less equals than data sent by publisher

			if _, found := innerContentMatch["$lte"]; found {
			    
			   	var lessThanEqualMap = innerContentMatch["$lte"].(map[string]interface{})

			   	for key := range lessThanEqualMap{

					var datatype = reflect.TypeOf(lessThanEqualMap[key])

					var datatype_msg = reflect.TypeOf(messageData[key])

					if datatype.Kind() == reflect.Float64 && datatype_msg.Kind() == reflect.Float64{

						if diff := math.Abs(lessThanEqualMap[key].(float64) - messageData[key].(float64)); diff < TOLERANCE{

							return true

						}

						if lessThanEqualMap[key].(float64) <= messageData[key].(float64){
						
							totalCount += 1

						}

					}

			   	}

			}

		}

		if totalCount == contentMatchLen{

			return false

		}else{

			return true

		}

	}else{

		return false

	}

}
