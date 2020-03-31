package main

import (
  "net"
  "time"
)

func main() {
  local,err1 := net.ResolveUDPAddr("udp4", ":8829")
  if err1 != nil {
    panic(err1)
  }
  remote,err2 := net.ResolveUDPAddr("udp4", "192.168.7.255:8829")
  if err2 != nil {
    panic(err2)
  }
  list, err3 := net.DialUDP("udp4", local, remote)
  if err3 != nil {
    panic(err3)
  }
  defer list.Close()

  for{

    time.Sleep(1 * time.Second)
    _,err4 := list.Write([]byte("data to transmit"))
    if err4 != nil {
      panic(err4)
    }
  }
}