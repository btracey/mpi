/*
gompirunlocal is a helper function for launching mpi jobs on a local machine.

Since Go is good at shared memory, generally programs should use Go's primitives
rather than MPI in a shared-memory environment. However, running locally can be
helpful for debugging and prototyping.

gompirun takes two arguments. The first argument is the command to run, and
the second argument is the number of instances to launch. Any additional
arguments will be passed to the program. Any shared memory
parallelism should be set in the program itself using runtime.GOMAXPROCS.

Instructions:
	go get github.com/btracey/mpi/mpirun/gompirun
	gompirun 8 programname -otherflag=value
*/

package main

import (
	"log"
	"os"
	"os/exec"
	"strconv"
	"sync"
)

func main() {
	if len(os.Args) < 3 {
		log.Fatal("less than two arguments, must have at least executable and number of nodes ")
	}
	nNodes, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatal("error parsing nNodes: ", err)
	}

	if nNodes < 1 {
		log.Fatal("number of nodes must be positive")
	}

	execName := os.Args[2]

	otherArgs := os.Args[3:]

	// Use local host ports
	baseport := 5000
	var ports []string
	for i := 0; i < nNodes; i++ {
		portName := ":" + strconv.Itoa(baseport+i)
		ports = append(ports, portName)
	}

	launch(execName, ports, otherArgs)
}

// Launch launches nNodes examples
func launch(execName string, ports []string, args []string) {
	// construct the full list of ports
	var portlist string
	for i, port := range ports {
		portlist += port
		if i != len(ports)-1 {
			portlist += ","
		}
	}
	wg := &sync.WaitGroup{}
	// Launch all of the commands
	for i, port := range ports {
		wg.Add(1)
		go func(i int, port string) {
			defer wg.Done()

			var a []string
			for _, v := range args {
				a = append(a, v)
			}
			a = append(a, "-mpi-addr", port, "-mpi-alladdr", portlist)
			cmd := exec.Command(execName, a...)
			cmd.Stdin = os.Stdin
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			cmd.Run()
		}(i, port)
	}
	wg.Wait()
}
