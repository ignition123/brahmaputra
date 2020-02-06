package keeper

import(
	"fmt"
	"pojo"
)

func ConnectClusters(configObj pojo.KeeperStruct){

	for index := range configObj.Clusters{
		
		for innerIndex := range configObj.Clusters[index]{

			fmt.Println(configObj.Clusters[index][innerIndex].Host)

		}

	}

}