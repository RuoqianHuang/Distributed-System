package main

import (
	"cs425/mp4/internal/stream"
	"log"
)

func main() {


	binPath := "/cs425/mp4/stage-1"
	
	op, err := stream.NewOperatorRunner(binPath, "none")
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	t := stream.Tuple{
		ID: "rainstorm_test:1",
		Key: "rainstorm_test:1",
		Value: "",
	}

	for i := 0; i < 10; i++ {
		tout, err := op.ProcessTuple(t)
		if err != nil {
			log.Printf("error: %v", err)
		}
		log.Printf("output: %v", tout)
	}
	
}
