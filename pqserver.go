package main

import "fmt"
import "bufio"
import "net"
//import "strings"
func main(){
	fmt.Println("Launching Population Query Server");
	ln, _ := net.Listen("tcp", ":8081");
	conn, _ := ln.Accept();

	message, _ := bufio.NewReader(conn).ReadString('\n');
	fmt.Print("Stuff received: ", string(message))
}
