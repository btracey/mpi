// Launches tasks from a slurm environment. Used with salloc
// use salloc -N
// currently assumes full nodes. Would need a config script otherwise
package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
)

func main() {
	if len(os.Args) == 1 {
		log.Fatal("mpirun_slurm must be called with the program name")
	}
	//progName := os.Args[1]
	nodelistStr := os.Getenv("SLURM_NODELIST")
	nodelistFancy := strings.Split(nodelistStr, " ")

	fmt.Println("node list str", nodelistStr)
	fmt.Println("len fancy ", len(nodelistFancy))
	var nodelist []string
	// Next, we need to see if there is a range of nodes
	for fancy := range nodelistFancy {
		strs := strings.Split(nodelistFancy[fancy], "[")
		if len(strs) == 1 {
			// No hyphen
			nodelist = append(nodelist, strs[0])
			continue
		}
		nodeRootName := strs[0]
		// Otherwise, need to find the numbers on either side of the hyphen and append them
		numStrs := strings.Split(strs[1], "-")
		// first element is a numbr
		lowInd, err := strconv.Atoi(numStrs[0])
		if err != nil {
			log.Fatal(err)
		}
		// High ind has a ] on teh end of it
		highStr := strings.TrimSuffix(numStrs[1], "]")
		highInd, err := strconv.Atoi(highStr)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("lowind = ", lowInd)
		fmt.Println("highind = ", highInd)
		for i := lowInd; i < highInd+1; i++ {
			nodeName := nodeRootName + strconv.Itoa(i)
			nodelist = append(nodelist, nodeName)
		}
	}

	fmt.Println("nodelist = ", nodelist)

	fullNodelist := ""
	for i := range nodelist {
		fullNodelist += nodelist[i] + ":5000"
		if i != len(nodelist)-1 {
			fullNodelist += ","
		}
	}

	fmt.Println("full nodelist = ", nodelist)

	wg := &sync.WaitGroup{}
	wg.Add(len(nodelist))
	for i := range nodelist {
		fmt.Println("i in node", i)
		go func(i int) {
			fmt.Println("Started goroutine")
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
