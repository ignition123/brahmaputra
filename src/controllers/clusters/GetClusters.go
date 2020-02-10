package clusters

import(
	"net/http"
	"node_keeper"
	"cns"
)

func GetClusters(input map[string]interface{}, req *http.Request,res http.ResponseWriter, cb chan interface{}){ 
	cns.SendMsg(cb, node_keeper.TCPClusters["clusters"])
}