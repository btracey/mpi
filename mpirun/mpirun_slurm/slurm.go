// Launches tasks from a slurm environment. Used with salloc
// use salloc -N
// currently assumes full nodes. Would need a config script otherwise
package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
)

func main() {
	if len(os.Args) == 1 {
		log.Fatal("mpirun_slurm must be called with the program name")
	}
	//progName := os.Args[1]
	nodelistStr := os.Getenv("SLURM_NODELIST")
	nodelist := strings.Split(nodelistStr, " ")
	fullNodelist := ""
	for i := range nodelist {
		fullNodelist += nodelist[i] + ":5000"
		if i != len(nodelist) {
			fullNodelist += ","
		}
	}

	wg := &sync.WaitGroup{}
	wg.Add(len(nodelist))
	for i := range nodelist {
		go func(i int) {
			defer wg.Done()
			args := []string{"-N1", os.Args[1]}
			for i := 2; i < len(os.Args); i++ {
				args = append(args, os.Args[i])
			}
			args = append(args, "-mpi-addr", nodelist[i]+":5000", "-mpi-alladdr", fullNodelist)
			fmt.Println("launch on: ", nodelist[i])
			exec.Command("srun", args...)
			fmt.Println("done executing on: ", nodelist[i])
		}(i)
	}
}
