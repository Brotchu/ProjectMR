package main

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"time"

	"github.com/Brotchu/ProjectMR/mr"
)

func main() {
	//os.Args (1) - local port to use (2) coordinator address
	if len(os.Args) < 3 {
		fmt.Println("Need (1) Local port to use and (2) Coordinator Address")
		os.Exit(1)
	}

	coordinatorAddress := os.Args[2]
	selfPort := os.Args[1]
	selfAddress := getLocalAddr() + ":" + selfPort

	client, err := rpc.DialHTTP("tcp", coordinatorAddress)
	mr.Must(err)

	var RegisterReply string //REGISTERING Worker with Coordinator
	err = client.Call("Coordinator.RegisterWorker", selfAddress, &RegisterReply)
	mr.Must(err)

	//Starting a Goroutine to listen to poll from coordinator
	go func() {
		acceptPoll := &Wapi{}
		mr.Must(rpc.Register(acceptPoll))
		lis, err := net.Listen("tcp", ":"+selfPort)
		mr.Must(err)
		mr.Must(http.Serve(lis, nil))
	}()

	//FIXME:
	// request to coordinator
	// var Mapreply string
	// err = client.Call("Coordinator.MapJob", selfAddress, &Mapreply)
	// fmt.Printf("%+v\n", Mapreply)
	// mr.Must(err)
	var req = mr.MapRequest{
		Data:     []mr.KeyValue{},
		WorkerId: selfAddress,
	}
	var reply mr.MapResponse
	for {
		err := client.Call("Coordinator.MapJob", req, &reply)
		fmt.Printf("reply from procedure %+v\n", reply)
		if err != nil {
			return
		}
		if reply.Status == false {
			break
		}
		time.Sleep(5 * time.Second)
	}
}

////////////////////Worker API for poll
type Wapi struct{}

func (w *Wapi) Poll(req string, reply *string) error {
	fmt.Println(req)
	*reply = "ok"
	return nil
}

////////////////////////////////////////////////////////

func getLocalAddr() string { //getting ip address of self to send to coordinator
	conn, err := net.Dial("udp", "8.8.8.8:80")
	mr.Must(err)
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)
	addrString := strings.Split(localAddr.String(), ":")
	return addrString[0]
}
