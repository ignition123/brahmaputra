package ChannelList

import(
    _"syscall"
    _"runtime"
    _"os"
)

// increasing ulimit only works in linux and darwin(mac) os, to be commented for windows as the objects are undefined

func SetUlimit(){

    // if runtime.GOOS == "linux" && *syscall.Rlimit != nil{

    //     var rLimit syscall.Rlimit

    //     err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)

    //     if err != nil{
    //         WriteLog("Error Getting Rlimit: "+err.Error())
    //         os.Exit(1)
    //     }

    //     if rLimit.Cur < rLimit.Max{

    //         rLimit.Cur = rLimit.Max

    //         err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)

    //         if err != nil{
    //             WriteLog("Error Setting Rlimit: "+err.Error())
    //             os.Exit(1)
    //         }

    //     }
    // }

}