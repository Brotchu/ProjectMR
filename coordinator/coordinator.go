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

//To sort Reduce out

type ByKeyR []mr.ReduceOut

func (a ByKeyR) Len() int           { return len(a) }
func (a ByKeyR) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKeyR) Less(i, j int) bool { return a[i].Key < a[j].Key }

type Coordinator struct {
	inputList    []string
	inputCount   int
	completedMap []string
	intrOut      []mr.KeyValue
	mapStatus    bool
	reduceInput  map[string][]string
	// reduceInput     []mr.ReduceRecord
	//make this a dict [string][]string line 100
	reduceInputKeys []string //take from this and put to reduce map
	// TODO: reduce map worker -> key : [string]string
	reduceInputCount int
	completedReduce  []string //list of keys completed
	reduceStatus     bool
	pluginName       string
	Result           map[string]string
	reduceOut        []mr.ReduceOut
	workerMap        map[string]string
	workerReduce     map[string]string
	Mut              *sync.Mutex
}

func (c *Coordinator) RegisterWorker(req string, reply *string) error {
	c.Mut.Lock()
	c.workerMap[req] = ""
	c.workerReduce[req] = ""
	// c.workerReduce[req] = mr.ReduceRecord{}
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
		c.reduceInput = make(map[string][]string)
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
			c.reduceInput[c.intrOut[iIter].Key] = tempValues
			// c.reduceInput = append(c.reduceInput, mr.ReduceRecord{ //FIXME:
			// 	Key:    c.intrOut[iIter].Key,
			// 	Values: tempValues,
			// })
			iIter = jIter
		}
		c.reduceInputCount = len(c.reduceInput)
		// c.completedReduce = []mr.ReduceRecord{}FIXME:
		c.reduceInputKeys = []string{}
		for kVal := range c.reduceInput {
			c.reduceInputKeys = append(c.reduceInputKeys, kVal)
		}
		c.completedReduce = []string{}
		c.reduceStatus = false
		c.Result = make(map[string]string)
		c.reduceOut = []mr.ReduceOut{}
		fmt.Printf("REDUCE INPUTS COUNT : %d\n", c.reduceInputCount)
		/////////////////////////////////////////////////
	}
	c.Mut.Unlock()

	reply.Input = newIn
	reply.Status = newStatus
	fmt.Printf("[REPLY] %+v\n", reply)

	return nil
}

func (c *Coordinator) ReduceJob(req mr.ReduceRequest, reply *mr.ReduceResponse) error {
	// fmt.Println(req)

	// reply.Status = true
	// reply.Record = mr.ReduceRecord{
	// 	Key:    c.reduceInputKeys[0],
	// 	Values: c.reduceInput[c.reduceInputKeys[0]],
	// }
	c.Mut.Lock()
	if c.reduceStatus {
		reply.Status = false
		reply.Record.Key = ""
		reply.Record.Values = []string{}
		return nil
	}
	if req.Key != "" {
		c.completedReduce = append(c.completedReduce, req.Key)
		c.Result[req.Key] = req.Result
		c.reduceOut = append(c.reduceOut, mr.ReduceOut{
			Key: req.Key,
			Res: req.Result,
		})
		if len(c.completedReduce) == c.reduceInputCount {
			c.reduceStatus = true
			sort.Sort(ByKeyR(c.reduceOut))          //Sort the data
			reduceFile, err := os.Create("out.txt") //open file to write out
			mr.Must(err)
			defer reduceFile.Close()
			fmt.Println("Map Reduce Job complete")
			for _, rec := range c.reduceOut {
				_, err = reduceFile.WriteString(rec.Key + "\t" + rec.Res + "\n")
			}
		}
		// fmt.Println(req.Key, req.Result, c.reduceInputCount, len(c.completedReduce))
	}
	reply.Status = false
	newKey := ""
	if len(c.reduceInputKeys) != 0 {
		reply.Status = true
		newKey = c.reduceInputKeys[0]
		c.reduceInputKeys = c.reduceInputKeys[1:]
	}
	reply.Record.Key = newKey
	if newKey != "" {
		reply.Record.Values = c.reduceInput[newKey]
	}
	c.Mut.Unlock()
	// reply.Record = c.reduceInput[0]
	// if len(c.reduceInput) == 0 {
	// 	reply.Record = mr.ReduceRecord{}
	// } else {
	// 	reply.Record = c.reduceInput[0]
	// 	c.reduceInput = c.reduceInput[1:]
	// }

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
		workerReduce: make(map[string]string),
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
		if len(c.workerMap) == 0 && c.reduceStatus {
			fmt.Println("MapReduce Complete")
			os.Exit(0)
		}
		for k := range c.workerMap {
			_, err := rpc.Dial("tcp", k) //Dial to worker
			if err != nil {
				fmt.Printf("[Unreachable] %s =>%v\n", k, err)
				if c.workerMap[k] != "" {
					c.inputList = append(c.inputList, c.workerMap[k])
				}
				if c.workerReduce[k] != "" {
					c.reduceInputKeys = append(c.reduceInputKeys, c.workerReduce[k])
				}
				// var emptyRec mr.ReduceRecord
				// if c.workerReduce[k] != emptyRec {

				// }
				delete(c.workerMap, k)
			}
		}
		c.Mut.Unlock()
		fmt.Printf("Workers : %+v\n", c.workerMap)
	}
}
