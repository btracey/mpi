// Launches tasks from a slurm environment. Used with salloc
// use salloc -N
// currently assumes full nodes. Would need a config script otherwise
package main

import (
	"log"
	"os"
	"os/exec"
	"strings"
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

	for i := range nodelist {

		args := []string{"-N1", os.Args[1]}
		for i := 2; i < len(os.Args); i++ {
			args = append(args, os.Args[i])
		}
		args = append(args, "-mpi-addr", nodelist[i]+":5000", "-mpi-alladdr", fullNodelist)
		exec.Command("srun", args...)
	}
}
