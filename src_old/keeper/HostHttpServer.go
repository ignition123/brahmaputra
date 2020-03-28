package keeper

import(
	"cns"
	"Web"
	"net/http"
)

func HostHTTPServer(httpNode map [string]interface{}){

	var port = httpNode["port"].(string)

	var http_secure = httpNode["secure"].(bool)

	var ssl = httpNode["ssl"].(map[string]interface{})
	var key = ssl["key"].(string)
	var crt = ssl["crt"].(string)

	httpApp := cns.Http{}

	Web.Routes()

	if http_secure{

		go func(){
			defer cns.CreateHttpsServer(":"+port, crt, key)
		}()

	}else{

		go func(){
			defer cns.CreateHttpServer(":"+port)
		}()

		defer httpApp.DefaultMethod(func(req *http.Request,res http.ResponseWriter){
			res.Header().Set("Name", "Sudeep Dasgupta")
			res.Header().Set("Content-Type", "application/json")
			res.Header().Set("Access-Control-Allow-Origin", "*")
			res.Header().Set("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept")
		})

	}
}