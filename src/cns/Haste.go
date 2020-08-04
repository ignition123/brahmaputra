package cns

/*
	Import packages
*/

import (
	"context"
	"log"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
	"math/rand"
	"encoding/json"
)

var mutex = &sync.RWMutex{}

// This is where go process is invoked

func init(){

	defer Recover()

	log.Println("Haste framework initiated, GLHF")

	if Config["MAXPROCS"] == "MAX"{

		runtime.GOMAXPROCS(runtime.NumCPU())

	}else{

		procs, err := strconv.Atoi(Config["MAXPROCS"])

		if err == nil{

			runtime.GOMAXPROCS(1)

		}else{

			runtime.GOMAXPROCS(procs)

		}

	}

}

// error handling

func Recover(){	

    if err := recover(); err != nil {

       	log.Println(err)
   	}

}

/*
	throw is used to throw exception using panic function and will recieved by recover
*/

func Throw(up Exception){

	panic(up)

}

/*
	Do function is created to initialize the whole try catch and finally method
*/

func (tcf Block) Do(){

	defer Recover()

	if tcf.Finally != nil{

		defer tcf.Finally()

	}

	if tcf.Catch != nil{

		defer func(){

			defer Recover()

			if r := recover(); r != nil{

				tcf.Catch(r)

			}

		}()

		tcf.Try()
	}

}

func (v *Http) DefaultMethod(fn func(req *http.Request, res http.ResponseWriter)){

	defer Recover()

	defaultObject["default"] = defaultStruct{

		callbackFunc: fn,

	}

}

// handlehttp is used to handle http request

func handHttp(res http.ResponseWriter, req *http.Request){

	defer Recover()

	defer req.Body.Close()

	mutex.Lock()

	defer mutex.Unlock()

	if defaultObject != nil{

		defaultObject["default"].callbackFunc(req, res)

	}

	// if url match with global

	httpObj := Http{
		req: req,
		res: res,
	}

	if req.Method == "GET" {

		globalObject = globalGetObject

	} else if req.Method == "POST" {

		globalObject = globalPostObject

	} else if req.Method == "PUT" {

		globalObject = globalPutObject

	} else {

		globalObject = globalDeleteObject

	}

	routesLen := len(globalObject)

	if routesLen == 0{

		log.Println("Please create a route")

		return

	}

	dirFailed := 0

	dirListLen := len(dirList)

	for _, val := range dirList{

		val = strings.Replace(val, "/", "\\/", -1)

		match, _ := regexp.MatchString("(?i)"+val+"[a-z0-9A-Z\\.]*", req.URL.Path)

		if match{

			httpObj.accessDenied()

			return

		}else{

			dirFailed += 1

		}

	}

	if dirFailed == dirListLen{

		ext := filepath.Ext(req.URL.Path)

		if ext == ""{

			notMatched := 0

			urlJoin := ""

			for key, val := range globalObject{

				val.url = strings.Replace(val.url, "/", "\\/", -1)

				match, _ := regexp.MatchString("^"+val.url+"$", req.URL.Path)

				if match && req.Method == val.method{

					httpObj.matchedUrl = key

					key = strings.Replace(key, "[\\", "", 1)

					urlArr := strings.Split(req.URL.Path, "/")

					urlLen := len(urlArr)

					valArr := strings.Split(key, "/")

					valLen := len(valArr)

					if urlLen == valLen{

						for key, val := range urlArr{

							if val != ""{

								urlJoin += "&" + valArr[key] + "=" + val

							}

						}

						if req.URL.RawQuery == ""{

							req.URL.RawQuery = urlJoin

						}else{

							req.URL.RawQuery = req.URL.RawQuery + urlJoin

						}

					}

					parseChan := make(chan bool)

					if httpObj.invokeGlobalMiddlewares(parseChan){
						
						go httpObj.parseHttpRequest(parseChan)

						if <-parseChan == true{

							if httpObj.invokeMiddlewares(parseChan){

								httpObj.invokeMethod()

								close(parseChan)

								break

							}else{

								break

							}

						}

					}else{

						break

					}

				}else{

					notMatched += 1

				}

			}

			if notMatched == routesLen{

				httpObj.pageNotFound()

			}

		}else{

			httpObj.serveStaticFiles(ext)

		}

	}

}

func (v *Http) parseHttpRequest(parseChan chan bool){

	defer Recover()

	if v.req.Method == "POST" || v.req.Method == "PUT" || v.req.Method == "DELETE"{

		if v.req.Header.Get("Content-Type") == "" || v.req.Header.Get("Content-Type") == "multipart/form-data"{

			parseChan <- true

		}else{

			if v.req.Header.Get("Content-Type") == "application/json"{

				parseChan <- true

			}else{

				v.req.ParseForm()

				parseChan <- true

			}

		}

	}else{

		parseChan <- true

	}

}

func (v *Http) SaveFile(w http.ResponseWriter, file multipart.File, path string, fileChan chan bool){

	defer Recover()

	data, err := ioutil.ReadAll(file)

	if err != nil{

		log.Println(w, "%v", err)

		fileChan <- false

		return
	}

	err = ioutil.WriteFile(path, data, 0666)

	if err != nil {

		log.Println(w, "%v", err)

		fileChan <- false

		return
	}

	fileChan <- true
}

func (v *Http) invokeMethod(){

	defer Recover()

	globalObject[v.matchedUrl].callbackFunc(v.req, v.res)

}

func (v *Http) BlockDirectories(dir []string){

	defer Recover()

	dirList = dir

}

func readErrorFile(path string) ([]byte, error){

	defer Recover()

	b, err := ioutil.ReadFile(path) // just pass the file name

	if err != nil{

		return nil, err

	}

	return b, nil
}

// getFunc is used to handle get request with func parameters

func (v *Http) Get(url string, fn func(req *http.Request, res http.ResponseWriter)) ChainAndError{

	defer Recover()

	v.curPath = url

	globalGetObject[url] = globalStruct{
		method:       "GET",
		url:          url,
		callbackType: "func",
		callbackFunc: fn,
	}

	REQUESTMETHOD = "GET"

	return ChainAndError{v, nil}
}

// PostFunc is used to handle post request with func parameters

func (v *Http) Post(url string, fn func(req *http.Request, res http.ResponseWriter)) ChainAndError{

	defer Recover()

	v.curPath = url

	globalPostObject[url] = globalStruct{
		method:       "POST",
		url:          url,
		callbackType: "func",
		callbackFunc: fn,
	}

	REQUESTMETHOD = "POST"

	return ChainAndError{v, nil}
}

// PutFunc is used to handle post request with func parameters

func (v *Http) Put(url string, fn func(req *http.Request, res http.ResponseWriter)) ChainAndError{

	defer Recover()

	v.curPath = url

	globalPutObject[url] = globalStruct{
		method:       "PUT",
		url:          url,
		callbackType: "func",
		callbackFunc: fn,
	}

	REQUESTMETHOD = "PUT"

	return ChainAndError{v, nil}
}

// DeleteFunc is used to handle post request with func parameters

func (v *Http) Delete(url string, fn func(req *http.Request, res http.ResponseWriter)) ChainAndError{

	defer Recover()

	v.curPath = url

	globalDeleteObject[url] = globalStruct{
		method:       "DELETE",
		url:          url,
		callbackType: "func",
		callbackFunc: fn,
	}

	REQUESTMETHOD = "DELETE"

	return ChainAndError{v, nil}
}

// middlewares method is used to handle middlewares

func (v *Http) Middlewares(middlewares func(req *http.Request, res http.ResponseWriter, done chan bool)) ChainAndError{

	defer Recover()

	if REQUESTMETHOD == "GET" {

		tmp := globalGetObject[v.curPath]

		tmp.middlewares = append(tmp.middlewares, middlewares)

		globalGetObject[v.curPath] = tmp

	} else if REQUESTMETHOD == "POST" {

		tmp := globalPostObject[v.curPath]

		tmp.middlewares = append(tmp.middlewares, middlewares)

		globalPostObject[v.curPath] = tmp

	} else if REQUESTMETHOD == "PUT" {

		tmp := globalPutObject[v.curPath]

		tmp.middlewares = append(tmp.middlewares, middlewares)

		globalPutObject[v.curPath] = tmp

	} else {

		tmp := globalDeleteObject[v.curPath]

		tmp.middlewares = append(tmp.middlewares, middlewares)

		globalDeleteObject[v.curPath] = tmp

	}

	return ChainAndError{v, nil}
}

// middlewares method is used to handle middlewares

func (v *Http) GlobalMiddleWares(middlewares func(req *http.Request, res http.ResponseWriter, done chan bool)) ChainAndError{

	defer Recover()

	tmp := globalMiscObject["GlobalMiddleWares"]

	tmp.globalMiddlewares = append(tmp.globalMiddlewares, middlewares)

	globalMiscObject["GlobalMiddleWares"] = tmp

	return ChainAndError{v, nil}
}

// where method is used to parse url matching with the regular expression

func (v *Http) Where(regex map[string]string) ChainAndError{

	defer Recover()

	if REQUESTMETHOD == "GET"{

		tmp := globalGetObject[v.curPath]

		tmp.url = globalGetObject[v.curPath].url

		for key, value := range regex{

			tmp.url = strings.Replace(tmp.url, key, value, -1)

		}

		globalGetObject[v.curPath] = tmp

	}else if REQUESTMETHOD == "POST"{

		tmp := globalPostObject[v.curPath]

		tmp.url = globalPostObject[v.curPath].url

		for key, value := range regex{

			tmp.url = strings.Replace(tmp.url, key, value, -1)

		}

		globalPostObject[v.curPath] = tmp

	}else if REQUESTMETHOD == "PUT"{

		tmp := globalPutObject[v.curPath]

		tmp.url = globalPutObject[v.curPath].url

		for key, value := range regex{

			tmp.url = strings.Replace(tmp.url, key, value, -1)

		}

		globalPutObject[v.curPath] = tmp

	}else{

		tmp := globalDeleteObject[v.curPath]

		tmp.url = globalDeleteObject[v.curPath].url

		for key, value := range regex{

			tmp.url = strings.Replace(tmp.url, key, value, -1)

		}

		globalDeleteObject[v.curPath] = tmp

	}

	return ChainAndError{v, nil}
}

// setting schema

func (v *Http) CreateSchema(projectMap map[string]Projection){

	defer Recover()

	projectHM = projectMap

}

// method to check if channel is closed before sending message

func IsClosed(ch chan interface{}) bool{

	defer Recover()

	select {
		case <-ch:
			return true
		default:
	}

	return false
}

// send message method is used in projection api to send message over the channels

func SendMsg(ch chan interface{}, class interface{}){

	defer Recover()

	if(!IsClosed(ch)){

		ch <- class

	}

}

// handling the http2 streaming

func handleHttpStreaming(res http.ResponseWriter, req *http.Request){

	defer Recover()

	// creating hashmap for request parsing

	requestHM := make(map[string]interface{})

	// decoding json string to structure

	err := json.NewDecoder(req.Body).Decode(&requestHM)

	if err != nil {

		log.Println(err)

	    jsonData, _ := json.Marshal(&DefaultResponse{
	    	Status:false,
	    	Msg:"Oops, something went wrong",
	    })

		res.Write([]byte(jsonData))

        return
    }

    // iterating over the request

    for key, _ := range requestHM{

    	// creating response hashmap

    	responseHashMap := make(map[string]interface{})

		if _, ok := projectHM[key]; ok{

			// creating channel for getting response from the invoked methods

		    callbackChan := make(chan interface{})

		    // setting context timeout 

			timeoutTime := rand.Int31n(projectHM[key].Timeout)

			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutTime) * time.Millisecond)

			defer cancel()

			// invoking methods with goroutines

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

				responseHashMap[key] =  &DefaultResponse{
			    	Status:false,
			    	Msg:"Method "+key+" timed out",
			    }

			  case callback := <-callbackChan:

			    responseHashMap[key] = callback

		  	}

		  	// closing the channel

		  	close(callbackChan)

		}else{

			responseHashMap[key] =  &DefaultResponse{
		    	Status:false,
		    	Msg:"Method "+key+" does not exists",
		    }

		}

		// creating json string for streaming response with a splitter \r\n\r\n

		jsonData, _ := json.Marshal(responseHashMap)

		res.Write([]byte(string(jsonData)+"\r\n\r\n"))

		res.(http.Flusher).Flush()
    } 

}