package cns

import(
	"net/http"
	"github.com/gorilla/websocket"
)


var username = "hasteUser"

// RequestGlobalVariable

var REQUESTMETHOD = ""

var defaultObject = map[string]defaultStruct{}

//  block directories list

var dirList []string

// createserver is used to initialize the https server

var mux *http.ServeMux

/*
	Http structure set with http request and response object and current Path
*/

type Http struct {
	res        http.ResponseWriter
	req        *http.Request
	curPath    string
	matchedUrl string
	json       string
}

/*
	chainAndError structure is used to make chaining methods in golang eg: obj.where().middlewares().get()
*/

type ChainAndError struct {
	*Http
	error
}

/*
	globalstruct struct is used to set all routes values
*/

type globalStruct struct {
	method            string
	url               string
	callbackType      string
	callbackFunc      func(req *http.Request, res http.ResponseWriter)
	middlewares       []func(req *http.Request, res http.ResponseWriter, done chan bool)
	globalMiddlewares []func(req *http.Request, res http.ResponseWriter, done chan bool)
}

/*
	Block structure for error handling
*/

type Block struct {
	Try     func()
	Catch   func(Exception)
	Finally func()
}

/*
	Exception interface to get exception
*/

type Exception interface{}

// globalWhereMap is used to collect all regex of the routes

var globalWhereMap = map[string]globalRegex{}

// globalObject is used to collect all global structure

var globalObject = map[string]globalStruct{}

// globalMiscObject is used to collect all global structure

var globalMiscObject = map[string]globalStruct{}

// globalGetObject is used to collect all global structure

var globalGetObject = map[string]globalStruct{}

// globalPostObject is used to collect all global structure

var globalPostObject = map[string]globalStruct{}

// globalPutObject is used to collect all global structure

var globalPutObject = map[string]globalStruct{}

// globalDeleteObject is used to collect all global structure

var globalDeleteObject = map[string]globalStruct{}

/*
	global regex structure is used to set regular expressions url
*/

type globalRegex struct {
	regex map[string]string
}

type defaultStruct struct {
	callbackFunc func(req *http.Request, res http.ResponseWriter)
}

/*
	Projection Api For Golang Haste Framework.
	To check for node implementation go to the node projection api
*/

type Projection struct{

	Method func(input map[string]interface{}, req *http.Request, res http.ResponseWriter, cb chan interface{})
	
	Timeout int32
}

// creating hashmap for projection schema

var projectHM = map[string]Projection{}

// default response structure

type DefaultResponse struct {
    Status bool `json:"status"`
    Msg string `json:"msg"`
}

// creating upgrader object for the gorilla websocket

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

// declaring variable for http2 multiplexing

var muxStream *http.ServeMux