package main

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/Brotchu/ProjectMR/mr"
)

//To sort intermediate

type ByKey []mr.KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type Coordinator struct {
	inputList        []string
	inputCount       int
	completedMap     []string
	intrOut          []mr.KeyValue
	mapStatus        bool
	reduceInput      []mr.ReduceRecord
	reduceInputCount int
	pluginName       string
	workerMap        map[string]string
	Mut              *sync.Mutex
}

func (c *Coordinator) RegisterWorker(req string, reply *string) error {
	c.Mut.Lock()
	c.workerMap[req] = ""
	*reply = c.pluginName
	c.Mut.Unlock()

	fmt.Printf("Worker Registered\n")
	return nil
}

func (c *Coordinator) MapJob(req mr.MapRequest, reply *mr.MapResponse) error {
	fmt.Printf("Procedure invoked by %+v\n", req)

	c.Mut.Lock()
	if c.workerMap[req.WorkerId] != "" {
		c.completedMap = append(c.completedMap, c.workerMap[req.WorkerId])
	}
	if c.mapStatus == true {
		*reply = mr.MapResponse{
			Status: false,
			Input:  "",
		}
		c.workerMap[req.WorkerId] = ""
		return nil
	}
	// c.workerMap[req.WorkerId] = ""
	c.intrOut = append(c.intrOut, req.Data...)

	newIn := ""       //give next input file to worker
	newStatus := true //set status for more maps ?
	if len(c.inputList) > 0 {
		newIn = c.inputList[0]
		c.inputList = c.inputList[1:]
	}
	c.workerMap[req.WorkerId] = newIn
	fmt.Println("checking if completed")
	fmt.Printf("%+v\n", c.completedMap)
	if len(c.completedMap) == c.inputCount {
		fmt.Printf("completed count : %d and input count %d\n", len(c.completedMap), c.inputCount)
		c.mapStatus = true //mapStatus, if map phase finished
		newStatus = false

		//sort intermediate out
		sort.Sort(ByKey(c.intrOut))
		// fmt.Printf("SORTED \n %+v\n", c.intrOut)

		//Splitting into chunks for reduce records TODO:
		c.reduceInput = []mr.ReduceRecord{}
		iIter := 0
		for iIter < len(c.intrOut) {
			jIter := iIter + 1
			for jIter < len(c.intrOut) && c.intrOut[iIter].Key == c.intrOut[jIter].Key {
				jIter++
			}
			tempValues := []string{}
			for kIter := iIter; kIter < jIter; kIter++ {
				tempValues = append(tempValues, c.intrOut[kIter].Value)
			}
			c.reduceInput = append(c.reduceInput, mr.ReduceRecord{
				Key:    c.intrOut[iIter].Key,
				Values: tempValues,
			})
			iIter = jIter
		}
		c.reduceInputCount = len(c.reduceInput)
		fmt.Printf("REDUCE INPUTS COUNT : %d\n", c.reduceInputCount)
		/////////////////////////////////////////////////
	}
	c.Mut.Unlock()

	reply.Input = newIn
	reply.Status = newStatus
	fmt.Printf("[REPLY] %+v\n", reply)

	return nil
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Need (1) plugin name (2..) list of input files")
		os.Exit(1)
	}
	fmt.Println("Starting Coordinator")
	inputFiles := os.Args[2:]
	plName := os.Args[1]

	var api = &Coordinator{
		inputList:    inputFiles,
		inputCount:   len(inputFiles),
		completedMap: []string{},
		intrOut:      []mr.KeyValue{},
		mapStatus:    false,
		pluginName:   plName,
		workerMap:    make(map[string]string),
		Mut:          &sync.Mutex{},
	}

	mr.Must(rpc.Register(api))
	rpc.HandleHTTP()

	//Start goroutine to poll workers
	go PollWorkers(api)

	lis, err := net.Listen("tcp", ":4040")
	mr.Must(err)
	fmt.Println("Coordinator running on 4040")
	mr.Must(http.Serve(lis, nil))
}

func PollWorkers(c *Coordinator) {
	for {
		time.Sleep(10 * time.Second)

		c.Mut.Lock()
		for k := range c.workerMap {
			_, err := rpc.Dial("tcp", k) //Dial to worker
			if err != nil {
				fmt.Printf("[Unreachable] %s =>%v\n", k, err)
				if c.workerMap[k] != "" {
					c.inputList = append(c.inputList, c.workerMap[k])
				}
				delete(c.workerMap, k)
			}
		}
		c.Mut.Unlock()
		fmt.Printf("Workers : %+v\n", c.workerMap)
	}
}
