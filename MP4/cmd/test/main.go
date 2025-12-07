package main

import (
	"cs425/mp4/internal/hydfs"
	"cs425/mp4/internal/stream"
	"fmt"
	"log"
	"sync"
	"time"
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
	
	filename := "test-11"

	storage, err := hydfs.NewClient()
	if err != nil {
		log.Fatalf("Fail to get hydfs client: %v", err)
	}


	storage.CreateEmpty(filename)
	wg := new(sync.WaitGroup)

	for i := 0; i < 2000; i++ {
		
		wg.Add(1)
		go func() {
			tout, err := op.ProcessTuple(t)
			if err != nil {
				log.Printf("error: %v", err)
			}
			log.Printf("output: %v", tout)

			err = storage.Append(filename, fmt.Sprintf("PROCESSED:%s", t.ID))
			if err != nil {
				log.Printf("Error appending to %s: %v", filename, err)
			} else {
				log.Printf("Success!")
			}
			wg.Done()
		}()

		// time.Sleep(1000 * time.Millisecond)
	}
	time.Sleep(time.Millisecond)
	wg.Wait()

	storage.Close()
	
}
