/*
This example is the basic hello world program for the go mpi implementation.
The program first initializes the mpi implementation and then finds its own rank.
It then concurrently sends and receives messages from all other processes in
the computation.

To run:
go get github.com/btracey/mpi
go install github.com/btracey/mpi/examples/helloworld

Then, in three different terminals, run one each of
	helloworld -mpi-addr=":5000" -mpi-alladdr=":5000,:5001,:5003"
	helloworld -mpi-addr=":5001" -mpi-alladdr=":5000,:5001,:5003"
	helloworld -mpi-addr=":5003" -mpi-alladdr=":5000,:5001,:5003"
*/
package main

import (
	"flag"
	"fmt"
	"log"
	"runtime"
	"sync"

	"github.com/btracey/mpi"
)

func main() {

	runtime.GOMAXPROCS(runtime.NumCPU())
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
	fmt.Printf("Hello world, I'm node %v in a land with %v nodes\n", rank, size)

	// Send and receive messages to and from everyone concurrently
	wg := &sync.WaitGroup{}
	wg.Add(size)
	for i := 0; i < size; i++ {
		go func(i int) {
			defer wg.Done()
			str := fmt.Sprintf("\"Hello node %v, I'm node %v\"", i, rank)
			if i == rank {
				str = "\"I'm just talking to myself\""
			}
			err := mpi.Send(str, i, 0)
			if err != nil {
				log.Fatal(err)
			}
		}(i)
	}
	wg.Add(size)
	for i := 0; i < size; i++ {
		go func(i int) {
			defer wg.Done()
			var str string
			err := mpi.Receive(&str, i, 0)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Printf("I, node %v, received a message: %v\n", rank, str)
		}(i)
	}
	wg.Wait()
}
