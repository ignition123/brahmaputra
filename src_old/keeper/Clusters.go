package keeper

import(
	"fmt"
	"net"
	"strconv"
	"time"
	"encoding/json"
	"bytes"
	"encoding/binary"
	"node_keeper"
)

func ConnectTCPClusters(){

	var clustersList = node_keeper.TCPClusters["clusters"].(map[string]interface{})

	for key := range clustersList{

		var cluster = clustersList[key].([]interface {})

		 node_keeper.TCPClusterTable[key] = make(map[string] interface{})

		go BindTCPCluster(cluster, key)

		go SendReplicaConnectionDetails(cluster)
	}
}

func BindTCPCluster(cluster []interface {}, key string){

	var tcpCluster =  node_keeper.TCPClusterTable[key].(map[string] interface{})

	tcpCluster["totalNodes"] = len(cluster)
	tcpCluster["parentIndex"] = -1

	var totalConnectedNodes = 0

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
			go RetryTCPConnection(cluster,  node_keeper.TCPClusterTable[key].(map[string] interface{}), clusterObj, key, index)
			continue
		}

		totalConnectedNodes += 1

		tcpCluster["totalConnected"] = totalConnectedNodes

		if tcpCluster["parentIndex"] == -1{
			tcpCluster["parentIndex"] = index 
			clusterObj["status"] = "ACTIVE"
			clusterObj["clusterMode"] = "PARENT"
			clusterObj["connObj"] = conn
		}else{
			clusterObj["status"] = "ACTIVE"
			clusterObj["clusterMode"] = "CHILD"
			clusterObj["connObj"] = conn
		}

		fmt.Println("Connected to "+key+" host number "+(strconv.Itoa(index + 1)))
		fmt.Printf("Connected to %s...\n", dest)

		go TCPListenForChange(cluster, clusterObj, key, index)
	}
}

func SendReplicaConnectionDetails(cluster []interface {}){

	var halt = false

	for{

		if !halt{

			halt = true

			for index := range cluster{

				var clusterObj = cluster[index].(map[string]interface{})

				if clusterObj["connObj"] == nil{
					continue
				}

				var nodeConnection = clusterObj["connObj"].(net.Conn)

				if nodeConnection != nil{

					var response = make(map[string]interface{})
					response["type"] = "cluster_list"
					response["channelName"] = "cluster_list"
					response["clustersList"] = node_keeper.TCPClusters["clusters"]

					jsonData, err := json.Marshal(response)

					if err != nil{
						go WriteLog(err.Error())
						continue
					}

					if node_keeper.TCPClusters["logPrint"].(bool){
						fmt.Println(string(jsonData))
					}

					var packetBuffer bytes.Buffer
					sizeBuff := make([]byte, 4)
					binary.LittleEndian.PutUint32(sizeBuff, uint32(len(jsonData)))
					packetBuffer.Write(sizeBuff)
					packetBuffer.Write(jsonData)

					_, err = nodeConnection.Write(packetBuffer.Bytes())

					if err != nil {
						go WriteLog(err.Error())
					}
				}	
			}

			time.Sleep(2 * time.Second)

			halt = false
		}

	}

}

func TCPListenForChange(cluster []interface {}, clusterObj map[string]interface{}, key string, index int){

	if clusterObj["connObj"] == nil{

		return

	}

	var messageMap = make(map[string]interface{})
	messageMap["type"] = "heart_beat"
	messageMap["channelName"] = "heart_beat"

	var nodeConnection = clusterObj["connObj"].(net.Conn)
	defer nodeConnection.Close()

	var halt = false

	for{

		if !halt{

			halt = true

			time.Sleep(2 * time.Second)

			jsonData, err := json.Marshal(messageMap)

			if err != nil{
				go WriteLog(err.Error())
				continue
			}

			var packetBuffer bytes.Buffer

			sizeBuff := make([]byte, 4)
			binary.LittleEndian.PutUint32(sizeBuff, uint32(len(jsonData)))
			packetBuffer.Write(sizeBuff)
			packetBuffer.Write(jsonData)

			var buffer []byte

			if !checkConnection(nodeConnection, buffer){

				parentStatus := clusterObj["clusterMode"].(string)

				disConnectedNode(cluster,  node_keeper.TCPClusterTable[key].(map[string] interface{}), clusterObj, parentStatus)
					
				fmt.Println("disconnected")

				go RetryTCPConnection(cluster,  node_keeper.TCPClusterTable[key].(map[string] interface{}), clusterObj, key, index)

				break
			
			}else{

				nodeConnection.SetWriteDeadline(time.Now().Add(1 * time.Second))

				_, err = nodeConnection.Write(packetBuffer.Bytes())

				if err != nil {
			
					go WriteLog(err.Error())

				}else{

					halt = false

				}

			}

		}

	}

}

func disConnectedNode(cluster []interface {}, tcpCluster map[string] interface{}, clusterObj map[string]interface{}, parentStatus string){

	clusterObj["status"] = "INACTIVE"
	clusterObj["clusterMode"] = "CHILD"
	clusterObj["connObj"] = nil

	var disconnectCount = tcpCluster["totalConnected"].(int)
	disconnectCount -= 1
	tcpCluster["totalConnected"] = disconnectCount

	if parentStatus == "PARENT"{

		for index := range cluster{

			var clusterObj = cluster[index].(map[string]interface{})

			if clusterObj["status"] == "ACTIVE"{

				clusterObj["clusterMode"] = "PARENT"
				tcpCluster["parentIndex"] = index

			}

		} 

	}

}

func connectedNode(conn net.Conn,cluster []interface {}, tcpCluster map[string] interface{}, clusterObj map[string]interface{}, key string, index int){

	var disconnectCount = tcpCluster["totalConnected"].(int)
	disconnectCount += 1
	tcpCluster["totalConnected"] = disconnectCount

	clusterObj["status"] = "ACTIVE"
	clusterObj["connObj"] = conn

	var parentFound = false

	for index := range cluster{

		var clusterObj = cluster[index].(map[string]interface{})

		if clusterObj["clusterMode"] == "PARENT"{

			parentFound = true

		}

	}

	if parentFound{
		clusterObj["clusterMode"] = "CHILD"
	}else{
		clusterObj["clusterMode"] = "PARENT"
	}

	go TCPListenForChange(cluster, clusterObj, key, index)

}

func checkConnection(c net.Conn, buffer []byte) bool {

    _, err := c.Read(buffer)
    
    if err != nil {
        c.Close()
        return false
    }

    return true
}

func RetryTCPConnection(cluster []interface {}, tcpCluster map[string] interface{}, clusterObj map[string]interface{}, key string, index int){

	RECONNECT: dest := clusterObj["host"].(string) + ":" + clusterObj["port"].(string)

	fmt.Println("Connecting to "+key+" host number "+(strconv.Itoa(index + 1)))

	conn, err := net.Dial("tcp", dest)	

	if err != nil {
		
		time.Sleep(2 * time.Second)

		goto RECONNECT

		return
	}

	go connectedNode(conn, cluster, tcpCluster, clusterObj, key, index)

	fmt.Println("Connected to "+key+" host number "+(strconv.Itoa(index + 1)))
	fmt.Printf("Connected to %s...\n", dest)
}