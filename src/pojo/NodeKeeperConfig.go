package pojo

type KeeperStruct struct{
	Host *string
	Port *string 
	Clusters [][] ClustersStruct
}

type ClustersStruct struct{
	Host *string
	Port *string 
}