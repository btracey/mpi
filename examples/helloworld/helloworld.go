package main

import (
	"flag"
	"fmt"
	"log"
	"sync"

	"github.com/btracey/mpi"
)

func main() {
	// Parse must be called to set the addresses
	flag.Parse()

	// Initialize the MPI routines and make sure
	err := mpi.Init()
	if err != nil {
		log.Fatal(err)
	}
	defer mpi.Finalize()

	rank := mpi.Rank()
	if rank == -1 {
		log.Fatal("Incorrect initialization")
	}
	size := mpi.Size()
	fmt.Printf("Hello world, my rank is %v out of %v\n", rank, size)

	// Send and receive messages to and from everyone concurrently
	wg := &sync.WaitGroup{}
	wg.Add(size)
	for i := 0; i < mpi.Size(); i++ {
		go func(i int) {
			defer wg.Done()
			str := fmt.Sprintf("\"Hello node %v, I'm node %v\"", i, rank)
			if i == rank {
				str = "\"Look, I'm talking to myself\""
			}
			err := mpi.Send(str, i, 0)
			if err != nil {
				log.Fatal(err)
			}
			mpi.Wait(i, 0)
		}(i)
	}
	wg.Add(size)
	for i := 0; i < mpi.Size(); i++ {
		go func(i int) {
			defer wg.Done()
			var str string
			err := mpi.Receive(&str, i, 0)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Println("Received a message: ", str)
		}(i)
	}
	wg.Wait()
}
