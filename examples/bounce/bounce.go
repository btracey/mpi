/*
Bounce tests the speed and latency of the underlying network. It sends a suite
of messages between even-odd pairs of nodes in the network. This problem must
be run on an even number of nodes.

To run on a single machine:
go get github.com/btracey/mpi
go install github.com/btracey/mpi/examples/bounce

Then, in four different terminals, run one each of
	bounce -mpi-addr=":5000" -mpi-alladdr=":5000,:5001,:5003,:5004"
	bounce -mpi-addr=":5001" -mpi-alladdr=":5000,:5001,:5003,:5004"
	bounce -mpi-addr=":5003" -mpi-alladdr=":5000,:5001,:5003,:5004"
	bounce -mpi-addr=":5003" -mpi-alladdr=":5000,:5001,:5003,:5004"
	bounce -mpi-addr=":5004" -mpi-alladdr=":5000,:5001,:5003,:5004"
*/
package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/btracey/mpi"
)

// length of message. Must be in increasing order
var msgLengths = []int{0, 1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7}

var nRepeats int64 = 50

func main() {
	rand.Seed(time.Now().UnixNano())
	flag.Parse()

	// Initialize MPI
	err := mpi.Init()
	if err != nil {
		log.Fatal("error initializing: ", err)
	}
	defer mpi.Finalize()

	// Second check that things started okay
	rank := mpi.Rank()
	if rank < 0 {
		log.Fatal("Incorrect initialization")
	}
	evenRank := rank%2 == 0

	size := mpi.Size()
	if size%2 != 0 {
		log.Fatal("Must have an even number of nodes for this example")
	}
	if rank == 0 {
		fmt.Println("Number of nodes = ", size)
	}

	// Make a random vector of bytes
	maxsize := msgLengths[len(msgLengths)-1]
	message := make([]byte, maxsize)
	//fmt.Println("...")
	for i := 0; i < maxsize/8; i++ {
		v := rand.Int63()
		binary.LittleEndian.PutUint64(message[i*8:], uint64(v))
	}

	receive := make([]byte, maxsize)

	// Do a call and response between the even and the odd nodes
	// and record the time. The even nodes will send a message to the next
	// node. The odd nodes will send that message back, which will be received
	// by the even node.
	times := make([]int64, len(msgLengths))
	for i, l := range msgLengths {
		for j := int64(0); j < nRepeats; j++ {
			msg := message[:l]
			rcv := receive[:l]
			start := time.Now()

			if evenRank {
				mpi.Send(msg, rank+1, 0)
			} else {
				mpi.Receive(&rcv, rank-1, 0)
			}

			if evenRank {
				mpi.Receive(&rcv, rank+1, 0)
			} else {
				mpi.Send(rcv, rank-1, 0)
			}

			times[i] += time.Since(start).Nanoseconds()

			// verify that the received message is the same as the sent message
			if evenRank {
				/*
					if !floats.Equal(msg, rcv) {
						log.Fatal("message not the same")
					}
				*/
				if !bytes.Equal(msg, rcv) {
					log.Fatal("message not the same")
				}
			}
			// Zero out the buffer so we don't get false positives next time
			for i := range rcv {
				rcv[i] = 0
			}
		}
	}

	// Convert the times to microseconds
	for i := range times {
		times[i] /= time.Microsecond.Nanoseconds()
		times[i] /= nRepeats
	}

	// Have the even nodes print their trip time in microseconds
	if evenRank {
		fmt.Printf("Average trip time in µs between node %d and %d: %v\n", rank, rank+1, times)
	}
}
