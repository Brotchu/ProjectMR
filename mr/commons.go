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

func Must(err error) {
	if err != nil {
		log.Fatal("[ERR] ", err)
		os.Exit(1)
	}
}
