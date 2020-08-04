package cns

import(
	"net/http"
	"os"
	"log"
	"context"
	"time"
	"encoding/json"
	"math/rand"
)

func CreateHttpsServer(hostPort string, crt string, key string){

	defer Recover()

	mux = http.NewServeMux()

	go mux.HandleFunc("/", handHttp)

	log.Println("Welcome to Golang Haste Framework ########*****##### Copy-Right 2020, Open Source Project")

	log.Println("To get source code visit, github.com/pounze/go_haste or www.pounze.com/pounze/go_haste")

	log.Println("Developed by Sudeep Dasgupta(Pounze)")

	log.Println("Server started->", hostPort)

	pwd, err := os.Getwd()

	if err != nil{

		log.Println(err)

		return
	}

	serverErr := http.ListenAndServeTLS(hostPort, pwd+crt, pwd+key, http.HandlerFunc(func(res http.ResponseWriter, req *http.Request){

		context := context.WithValue(req.Context(), "username", username)

		mux.ServeHTTP(res, req.WithContext(context))

	}))

	if serverErr != nil{

		log.Println(serverErr)

	}

}

// createserver is used to initialize the http server

func CreateHttpServer(hostPort string){

	defer Recover()

	mux = http.NewServeMux()

	go mux.HandleFunc("/", handHttp)

	log.Println("Welcome to Golang Haste Framework ########*****##### Copy-Right 2020, Open Source Project")

	log.Println("To get source code visit, github.com/pounze/go_haste or www.pounze.com/pounze/go_haste")

	log.Println("Developed by Sudeep Dasgupta(Pounze)")

	log.Println("Server started->", hostPort)

	serverErr := http.ListenAndServe(hostPort, http.HandlerFunc(func(res http.ResponseWriter, req *http.Request){

		context := context.WithValue(req.Context(), "username", username)

		mux.ServeHTTP(res, req.WithContext(context))

	}))

	if serverErr != nil{

		log.Println(serverErr)

	}

}

// createserver is used to initialize the http2 server

func CreateHttp2Server(hostPort string, crt string, key string){

	defer Recover()

	mux = http.NewServeMux()

	go mux.HandleFunc("/", handHttp)

	log.Println("Welcome to Golang Haste Framework ########*****##### Copy-Right 2020, Open Source Project")

	log.Println("To get source code visit, github.com/pounze/go_haste or www.pounze.com/pounze/go_haste")

	log.Println("Developed by Sudeep Dasgupta(Pounze)")

	log.Println("Server started->", hostPort)

	pwd, err := os.Getwd()

	if err != nil{

		log.Println(err)

		return

	}

	serverErr := http.ListenAndServeTLS(hostPort, pwd+crt, pwd+key, http.HandlerFunc(func(res http.ResponseWriter, req *http.Request){

		context := context.WithValue(req.Context(), "username", username)

		mux.ServeHTTP(res, req.WithContext(context))

	}))

	if serverErr != nil{

		log.Println(serverErr)

	}

}

func (v *Http) serveStaticFiles(ext string){

	defer Recover()

	v.res.Header().Set("Content-Type", MimeList[ext])

	curTime := time.Now()

	v.res.Header().Set("Keep-Alive", "timeout=5, max=500")

	v.res.Header().Set("Server", "Go Haste Server")

	v.res.Header().Set("Developed-By", "Pounze It-Solution Pvt Limited")

	v.res.Header().Set("Expires", curTime.String())

	pwd, err := os.Getwd()

	if err != nil{

		log.Println(err)

		return
	}

	_, err = os.Stat(pwd + v.req.URL.Path)

	if err == nil{

		page, err := readErrorFile(pwd + v.req.URL.Path)

		if err != nil{

			v.pageNotFound()

		}else{

			lastModifiedDate := v.req.Header.Get("if-modified-since")

			file, err := os.Stat(pwd + v.req.URL.Path)

			if err != nil{

				v.pageNotFound()

			}else{

				modifiedtime := file.ModTime()

				if lastModifiedDate == ""{

					v.res.Header().Set("Last-Modified", modifiedtime.Format("2006-01-02 15:04:05"))

					v.res.WriteHeader(http.StatusOK)

				}else{

					time, err := time.Parse("2006-01-02 15:04:05", lastModifiedDate)

					if err != nil {

						v.pageNotFound()

					}else{

						v.res.Header().Set("Last-Modified", modifiedtime.Format("2006-01-02 15:04:05"))

						if time.Unix() < modifiedtime.Unix(){

							v.res.WriteHeader(http.StatusOK)

						}else{

							v.res.WriteHeader(http.StatusNotModified)

						}

					}

				}

			}

			v.res.Write(page)
		}

	}else{

		v.pageNotFound()

	}

}

func (v *Http) invokeGlobalMiddlewares(parseChan chan bool) bool{

	defer Recover()

	middlewareCount := 0

	if globalMiscObject["GlobalMiddleWares"].globalMiddlewares != nil{

		for index, _ := range globalMiscObject["GlobalMiddleWares"].globalMiddlewares{

			go globalMiscObject["GlobalMiddleWares"].globalMiddlewares[index](v.req, v.res, parseChan)

			if <-parseChan == false{

				middlewareCount += 1

			}

		}

		if middlewareCount > 0{
			return false

		}

	}

	return true
}

func (v *Http) invokeMiddlewares(parseChan chan bool) bool{

	defer Recover()

	middlewareCount := 0

	if globalObject[v.matchedUrl].middlewares != nil{

		for index, _ := range globalObject[v.matchedUrl].middlewares{
			
			go globalObject[v.matchedUrl].middlewares[index](v.req, v.res, parseChan)

			if <-parseChan == false{

				middlewareCount += 1

			}

		}

		if middlewareCount > 0{

			return false

		}
		
	}

	return true
}

// project method for http/1.1

func (v *Http) ProjectionMethod(req *http.Request,res http.ResponseWriter){

	defer Recover()

	// creating hashmap for response

	responseHashMap := make(map[string]interface{})

	// creating another hashmap to parse request json 

	requestHM := make(map[string]interface{})

	// decoding json request

	err := json.NewDecoder(req.Body).Decode(&requestHM)

	if err != nil{

	    jsonData, _ := json.Marshal(&DefaultResponse{
	    	Status:false,
	    	Msg:"Oops, something went wrong",
	    })

		res.Write([]byte(jsonData))

        return
    }

    // iterating over all the request schema in the request payload

	for key, _ := range requestHM{

		if _, ok := projectHM[key]; ok{

			// creating channels for callback

		    callbackChan := make(chan interface{})

		    // setting timeout for context

			timeoutTime := rand.Int31n(projectHM[key].Timeout)

			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutTime) * time.Millisecond)

			defer cancel()

			// invoking method with go routines

			md, ok := requestHM[key].(map[string]interface{})

			if !ok{

				responseHashMap[key] = &DefaultResponse{
			    	Status:false,
			    	Msg:"Method "+key+" does not contain valid json",
			    }

				continue
			}

	        go projectHM[key].Method(md, req, res, callbackChan)

	        // checking if context is done

	        select{

			  case <-ctx.Done():

				responseHashMap[key] = &DefaultResponse{
			    	Status:false,
			    	Msg:"Method "+key+" timed out",
			    }

			  case callback := <-callbackChan:

			    responseHashMap[key] = callback

		  	}

		  	// closing the channel

		  	close(callbackChan)

		}else{

			responseHashMap[key] = &DefaultResponse{
		    	Status:false,
		    	Msg:"Method "+key+" does not exists",
		    }

		}

    }

    // sending the http response

    jsonData, _ := json.Marshal(responseHashMap)

    res.Write([]byte(jsonData))
}

// set route path to set projection url

func (v *Http) SetRoutePath(path string) ChainAndError{

	defer Recover()

	v.Post(path, v.ProjectionMethod)

	return ChainAndError{v, nil}
}

// method to handle socket projection request

func (v *Http) SocketProjection(res http.ResponseWriter, req *http.Request){

	defer Recover()

	// upgrade method to upgrade http to websocket

	con, err := upgrader.Upgrade(res, req, nil)
	
	if err != nil {

		return

	}

	// closing the socket connection

	defer con.Close()
	
	for{

		// reading messages from sockets

		mt, message, err := con.ReadMessage()
		
		if err != nil{

			break

		}

		// creating hashmap  for parsing the request

		requestHM := make(map[string]interface{})

		// creating object from json string

		jsonError := json.Unmarshal(message, &requestHM)

		if jsonError != nil{

		    jsonData, _ := json.Marshal(&DefaultResponse{
		    	Status:false,
		    	Msg:"Oops, something went wrong",
		    })

	        writeError := con.WriteMessage(mt, []byte(jsonData))

	        if writeError != nil{

				break

			}

	    }

	    // iterating over the json object

	    for key, _ := range requestHM{

	    	responseHashMap := make(map[string]interface{})
	    	
			if _, ok := projectHM[key]; ok{

				// creating channel to get the response

			    callbackChan := make(chan interface{})

			    // setting context timeout

				timeoutTime := rand.Int31n(projectHM[key].Timeout)

				ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutTime) * time.Millisecond)

				defer cancel()

				// invoking method for matched method from request payload

				md, ok := requestHM[key].(map[string]interface{})

				if !ok{

					responseHashMap[key] = &DefaultResponse{
				    	Status:false,
				    	Msg:"Method "+key+" does not contain valid json",
				    }

					continue
				}

		        go projectHM[key].Method(md, req, res, callbackChan)

		        select{

				  case <-ctx.Done():

					responseHashMap[key] = &DefaultResponse{
				    	Status:false,
				    	Msg:"Method "+key+" timed out",
				    }

				  case callback := <-callbackChan:

				    responseHashMap[key] = callback
			  	}

			  	// closing the channel

			  	close(callbackChan)

			}else{

				responseHashMap[key] = &DefaultResponse{
			    	Status:false,
			    	Msg:"Method "+key+" does not exists",
			    }

			}

			// creating json string and sending as response

			jsonData, _ := json.Marshal(responseHashMap)

    		writeError := con.WriteMessage(mt, []byte(jsonData))

	        if writeError != nil{

				break

			}

	    }
		
	}

}

// creating socket path for websocket streaming

func (v *Http) SetSocketRoutePath(path string) ChainAndError{

	defer Recover()
	
	go func(){

		defer Recover()

		for{
		
			if mux != nil{

				break

			}

	        time.Sleep(1 * time.Second)
	    }

		go mux.HandleFunc(path, v.SocketProjection)

	}()

	return ChainAndError{v, nil}
}

// creating http2 streaming, it has only support with TLS 

func CreateHttpStreaming(hostPort string, crt string, key string, path string){

	defer Recover()

	muxStream = http.NewServeMux()

	go muxStream.HandleFunc(path, handleHttpStreaming)

	log.Println("Welcome to Golang Haste Framework ########*****##### Copy-Right 2020, Open Source Project")

	log.Println("To get source code visit, github.com/pounze/go_haste or www.pounze.com/pounze/go_haste")

	log.Println("Developed by Sudeep Dasgupta(Pounze)")

	log.Println("Server started->", hostPort)

	pwd, err := os.Getwd()

	if err != nil{

		log.Println(err)

		return

	}

	serverErr := http.ListenAndServeTLS(hostPort, pwd+crt, pwd+key, http.HandlerFunc(func(res http.ResponseWriter, req *http.Request){

		context := context.WithValue(req.Context(), "username", username)

		muxStream.ServeHTTP(res, req.WithContext(context))

	}))

	if serverErr != nil{

		log.Println(serverErr)

	}

}
