package keeper

import(
	"fmt"
	"net"
	"strconv"
	"time"
)

func ConnectTCPClusters(){

	var clustersList = TCPClusters["clusters"].(map[string]interface{})

	for key := range clustersList{

		var cluster = clustersList[key].([]interface {})

		TCPClusterTable[key] = -1

		BindTCPCluster(cluster, key)
	}

	fmt.Println(TCPClusters)
	fmt.Println(TCPClusterTable)
}

func BindTCPCluster(cluster []interface {}, key string){

	for index := range cluster{

		var clusterObj = cluster[index].(map[string]interface{})
		
		dest := clusterObj["host"].(string) + ":" + clusterObj["port"].(string)

		fmt.Println("Connecting to "+key+" host number "+(strconv.Itoa(index + 1)))

		conn, err := net.Dial("tcp", dest)

		clusterObj["status"] = "INACTIVE"
		clusterObj["clusterMode"] = "CHILD"
		clusterObj["connObj"] = nil

		if err != nil {
			WriteLog("Error listening: "+err.Error())
			continue
		}

		conn.SetWriteDeadline(time.Now().Add(time.Second * 1))

		if TCPClusterTable[key] == -1{
			TCPClusterTable[key] = index
			clusterObj["status"] = "ACTIVE"
			clusterObj["clusterMode"] = "CHILD"
			clusterObj["connObj"] = conn
		}else{
			clusterObj["status"] = "ACTIVE"
			clusterObj["clusterMode"] = "CHILD"
			clusterObj["connObj"] = conn
		}

		fmt.Printf("Connected to %s...\n", dest)

		go TCPListenForChange(cluster, clusterObj, key, index)
	}

}

func TCPListenForChange(cluster []interface {}, clusterObj map[string]interface{}, key string, index int){

	var messageMap = make(map[string]interface{})
	messageMap["type"] = "heart_beat"

	var halt = false

	for{

		if !halt{

			halt = true

			time.Sleep(2 * time.Second)

			fmt.Println("ok")

			halt = false
		}

	}

}

func RetryTCPConnection(){

}