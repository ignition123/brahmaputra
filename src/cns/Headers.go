package cns

import(
	"time"
	"net/http"
	"os"
	"log"
)

/*
	Error Pages Static HTML Files
*/

func (v *Http) defaultHeaders(){

	defer Recover()

	curTime := time.Now()

	v.res.Header().Set("Content-Type", "text/html")

	v.res.Header().Set("Cache-Control", "public,max-age=31536000")

	v.res.Header().Set("Keep-Alive", "timeout=5, max=500")

	v.res.Header().Set("Server", "Go Haste Server")

	v.res.Header().Set("Developed-By", "Pounze It-Solution Pvt Limited")

	v.res.Header().Set("Pragma", "public,max-age=31536000")

	v.res.Header().Set("Expires", curTime.String())

}

// Page Not Found Page 404.html

func (v *Http) pageNotFound(){

	defer Recover()

	v.defaultHeaders()

	v.res.WriteHeader(http.StatusNotFound)

	pwd, err := os.Getwd()

	if err != nil{

		log.Println(err)

		return

	}

	_, err = os.Stat(pwd + "/src/error_files/404.html")

	if err == nil{

		page, err := readErrorFile(pwd + "/src/error_files/404.html")

		if err != nil{

			v.res.Write([]byte("404 Page Not Found"))

		}else{

			v.res.Write(page)

		}

	}else{

		v.res.Write([]byte("404 Page Not Found"))

	}

}

// Access Denies 403 Error File is Served

func (v *Http) accessDenied(){

	defer Recover()

	v.defaultHeaders()

	v.res.WriteHeader(http.StatusForbidden)

	pwd, err := os.Getwd()

	if err != nil{

		log.Println(err)

		return

	}

	_, err = os.Stat(pwd + "/src/error_files/403.html")

	if err == nil{

		page, err := readErrorFile(pwd + "/src/error_files/403.html")

		if err != nil{

			v.res.Write([]byte("403 Access Denied"))

		}else{

			v.res.Write(page)

		}

	}else{

		v.res.Write([]byte("403 Access Denied"))

	}

}

// Internal Server Error 500

func (v *Http) serverError(){

	defer Recover()

	v.defaultHeaders()

	v.res.WriteHeader(http.StatusInternalServerError)

	pwd, err := os.Getwd()

	if err != nil{

		log.Println(err)

		return

	}

	_, err = os.Stat(pwd + "/src/error_files/500.html")

	if err == nil{

		page, err := readErrorFile(pwd + "/src/error_files/500.html")

		if err != nil{

			v.res.Write([]byte("500 Internal Server Error"))

		}else{

			v.res.Write(page)

		}

	}else{

		v.res.Write([]byte("500 Internal Server Error"))

	}
	
}