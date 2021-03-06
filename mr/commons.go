package mr

import (
	"log"
	"os"
)

type KeyValue struct {
	Key   string
	Value string
}

type MapRequest struct {
	Data     []KeyValue
	WorkerId string
}

type MapResponse struct {
	Status bool
	Input  string
}

type ReduceRequest struct {
	Key      string
	Result   string
	WorkerId string
}

type ReduceResponse struct {
	Status bool
	Record ReduceRecord
}

type ReduceRecord struct {
	Key    string
	Values []string
}

type ReduceOut struct {
	Key string
	Res string
}

func Must(err error) {
	if err != nil {
		log.Fatal("[ERR] ", err)
		os.Exit(1)
	}
}
