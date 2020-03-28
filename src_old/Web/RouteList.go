package Web

import(
	"cns"
	"controllers/clusters"
	_"net/http"
)

func Routes(){

	httpApp := cns.Http{}

	httpApp.CreateSchema(map[string]cns.Projection{
	    "GetClusters": cns.Projection{
	        clusters.GetClusters,
	        1000,
	    },
	});

	httpApp.SetRoutePath("/get_cluster_list")

	httpApp.SetSocketRoutePath("/get_clusters")
}