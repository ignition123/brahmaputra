package MongoConnection

import(
	"context"
    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"
    "go.mongodb.org/mongo-driver/bson"
    "fmt"
    "ChannelList"
    "time"
    "io/ioutil"
	"path/filepath"
	"encoding/json"
)

var MongoDB *mongo.Database

func Connect() bool{

	defer ChannelList.Recover()

	/*
		Setting URL for mongodb connections
	*/

	clientOptions := options.Client().ApplyURI(*ChannelList.ConfigTCPObj.Storage.Mongodb.Url).SetMinPoolSize(*ChannelList.ConfigTCPObj.Storage.Mongodb.MinPoolSize).SetMaxPoolSize(*ChannelList.ConfigTCPObj.Storage.Mongodb.MaxPoolSize).SetRetryWrites(true)

	// Connecting to Mongodb with server host localhost and port 27017

	client, err := mongo.Connect(context.TODO(), clientOptions)

	if err != nil{
		fmt.Println("Failed to connect mongodb")
	    fmt.Println(err)
	    return false
	}

	/*
		Check the connection
	*/ 

	/*
		Checling for ping server if connected
	*/ 

	err = client.Ping(context.TODO(), nil)

	if err != nil{
	    fmt.Println(err)
	    return false
	}

	MongoDB = client.Database("brahmaputra")

	fmt.Println("Connected to MongoDB! on : ",*ChannelList.ConfigTCPObj.Storage.Mongodb.Url)

	return true
}

func SetupCollection() bool{

	defer ChannelList.Recover()

	files, err := ioutil.ReadDir(*ChannelList.ConfigTCPObj.ChannelConfigFiles)

    if err != nil {
        ChannelList.WriteLog(err.Error())
        return false
    }

    for _, file := range files{

    	extension := filepath.Ext(file.Name())

        if extension == ".json"{

        	data, err := ioutil.ReadFile(*ChannelList.ConfigTCPObj.ChannelConfigFiles+"/"+file.Name())

			if err != nil{
				ChannelList.WriteLog(err.Error())
				return false
			}

			channelMap := make(map[string]interface{})

			err = json.Unmarshal(data, &channelMap)

			if err != nil{
				ChannelList.WriteLog(err.Error())
				return false
			}

			var channelName = channelMap["channelName"].(string)

			if channelName == "heart_beat"{
				continue
			}

			ctx, _ := context.WithTimeout(context.Background(), 15 * time.Second)

			col := MongoDB.Collection(channelName)

			mod := mongo.IndexModel{
				Keys: bson.M{
				"offsetID": -1,
				}, Options: nil,
			}

			_, err = col.Indexes().CreateOne(ctx, mod)

			if err != nil{
				ChannelList.WriteLog(err.Error())
				return false
			}

			mod = mongo.IndexModel{
				Keys: bson.M{
				"cluster": 1,
				}, Options: nil,
			}

			_, err = col.Indexes().CreateOne(ctx, mod)

			if err != nil{
				ChannelList.WriteLog(err.Error())
				return false
			}

			ChannelList.WriteLog("Mongodb initiated successfully for channel name: "+channelName)

        }
    }

    return true
}

func InsertOne(collctionName string, oneDoc map[string]interface{}) (bool,interface{}){

	defer ChannelList.Recover()

	ctx := context.Background()
	
	col := MongoDB.Collection(collctionName)

	result, insertErr := col.InsertOne(ctx, oneDoc)

	if insertErr != nil {
		
		return false, insertErr.Error()

	}else{
		
		return true, result.InsertedID

	}

}

func Database() *mongo.Database{
	return MongoDB
}