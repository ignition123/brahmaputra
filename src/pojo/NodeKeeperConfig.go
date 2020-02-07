package pojo

import(
	"net"
)

type KeeperStruct struct{
	Host *string
	Port *string 
	Clusters [][] ClustersStruct
}

type ClustersStruct struct{
	Host *string
	Port *string
	Status *string
	ClusterMode *string 
	ConnObj net.Conn
}