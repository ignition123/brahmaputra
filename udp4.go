package main

import (
  "fmt"
  "net"
)

func main() {
  pc,err := net.ListenPacket("udp4", ":8829")
  if err != nil {
    panic(err)
  }
  defer pc.Close()

  buf := make([]byte, 1024)
  n,addr,err2 := pc.ReadFrom(buf)
  if err2 != nil {
    panic(err2)
  }
  
  fmt.Printf("%s sent this: %s\n", addr, buf[:n])
}