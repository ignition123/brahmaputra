package keeper

import(
	"fmt"
	"pojo"
)

func ConnectTCPClusters(configObj pojo.KeeperStruct){

	for index := range configObj.Clusters{
		
		for innerIndex := range configObj.Clusters[index]{

			go ConnectTCP(configObj.Clusters, index, innerIndex)

		}

	}

}

func ConnectTCP(configObj pojo.KeeperStruct, index int, innerIndex int){

	dest := *configObj[index][innerIndex].Host + ":" + strconv.Itoa(*configObj[index][innerIndex].Port)

	fmt.Printf("Connecting to %s...\n", dest)

	conn, err := net.Dial("tcp", dest)

	if err != nil {
		*configObj[index][innerIndex].Status = "INACTIVE"
		*configObj[index][innerIndex].ClusterMode = "CHILD"
		*configObj[index][innerIndex].ConnObj = nil
		WriteLog("Error listening: "+err.Error())
		return
	}

	*configObj[index][innerIndex].Status = "ACTIVE"
	*configObj[index][innerIndex].ClusterMode = "CHILD"
	*configObj[index][innerIndex].ConnObj = conn

	fmt.Printf("Connected to %s...\n", dest)

}