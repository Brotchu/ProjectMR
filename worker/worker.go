package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"plugin"
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
	//TODO: load plugin
	mapFunc, reduceFunc := loadPlugin(RegisterReply)
	fmt.Printf("%+v\n", mapFunc)

	//Starting a Goroutine to listen to poll from coordinator
	go func() {
		acceptPoll := &Wapi{}
		mr.Must(rpc.Register(acceptPoll))
		lis, err := net.Listen("tcp", ":"+selfPort)
		mr.Must(err)
		mr.Must(http.Serve(lis, nil))
	}()

	var req = mr.MapRequest{
		Data:     []mr.KeyValue{},
		WorkerId: selfAddress,
	}
	var reply mr.MapResponse
	for {
		reply = mr.MapResponse{}
		err := client.Call("Coordinator.MapJob", req, &reply)
		fmt.Printf("reply from procedure %+v\n", reply)
		if err != nil {
			return
		}
		if reply.Status == false {
			break
		}
		//set data empty again
		req.Data = []mr.KeyValue{}
		if reply.Input != "" { //do the map job
			file, err := os.Open(reply.Input)
			mr.Must(err)
			content, err := ioutil.ReadAll(file)
			mr.Must(err)
			file.Close()
			req.Data = mapFunc(reply.Input, string(content))
		}
		time.Sleep(5 * time.Second)
	}
	reduceReq := mr.ReduceRequest{
		Key:      "",
		Result:   "",
		WorkerId: selfAddress,
	}
	var reduceReply mr.ReduceResponse
	for {
		reduceReply = mr.ReduceResponse{}
		err := client.Call("Coordinator.ReduceJob", reduceReq, &reduceReply)
		if err != nil {
			fmt.Printf("[Err] : %+v", err)
			return
		}
		if reduceReply.Status == false {
			break
		}
		reduceReq.Key = reduceReply.Record.Key
		reduceReq.Result = reduceFunc(reduceReply.Record.Key, reduceReply.Record.Values)
		fmt.Println(reduceReply.Record.Key, reduceFunc(reduceReply.Record.Key, reduceReply.Record.Values))
		// fmt.Println(reduceReply)
		// break //FIXME:
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

func loadPlugin(pName string) (func(string, string) []mr.KeyValue, func(string, []string) string) { //Loading map and reduce(TODO:) according to plugin
	p, err := plugin.Open(pName)
	if err != nil {
		log.Fatal("Err: ", err)
		return nil, nil
	}

	f, err := p.Lookup("Map") //getting map function from plugin
	mFunc := f.(func(string, string) []mr.KeyValue)

	reducef, err := p.Lookup("Reduce")
	rFunc := reducef.(func(string, []string) string)
	return mFunc, rFunc
}
