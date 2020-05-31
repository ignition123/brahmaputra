package ChannelList

import(
    _"syscall"
    _"runtime"
    _"os"
)

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