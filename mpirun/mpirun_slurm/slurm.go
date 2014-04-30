// Launches tasks from a slurm environment. Used with salloc
// use salloc -N
// currently assumes full nodes. Would need a config script otherwise
package main

import (
	"flag"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
)

func main() {
	var nNodes int
	flag.IntVar(&nNodes, "n", 0, "number of nodes to use")
	var nCores int
	flag.IntVar(&nCores, "c", 0, "number of cores to use")
	flag.Parse()

	if nNodes == 0 {
		log.Fatal("n set to 0")
	}
	if nCores == 0 {
		log.Fatal("c set to 0")
	}

	salloc := exec.Command("salloc", "-n", strconv.Itoa(nNodes), "-c", strconv.Itoa(nCores))
	err := salloc.Run()
	if err != nil {
		log.Fatal(err)
	}
	jobId := os.Getenv("SLURM_JOB_ID")

	scancel := exec.Command("scancel", jobId)
	defer scancel.Run()

	if len(os.Args) == 1 {
		log.Fatal("mpirun_slurm must be called with the program name")
	}

	//progName := os.Args[1]
	nodelistStr := os.Getenv("SLURM_JOB_NODELIST")
	nodelistFancy := strings.Split(nodelistStr, " ")

	var nodelist []string
	// Next, we need to see if there is a range of nodes
	for fancy := range nodelistFancy {
		strs := strings.Split(nodelistFancy[fancy], "[")
		if len(strs) == 1 {
			// No hyphen
			nodelist = append(nodelist, strs[0])
			continue
		}
		// Otherwise, need to find the numbers on either side of the hyphen and append them
		nodeRootName := strs[0]
		other := strs[1]
		other = strings.TrimSuffix(other, "]")
		// Now, split at all of the commas again
		strs = strings.Split(other, ",")
		for _, sweepStr := range strs {
			numStrs := strings.Split(sweepStr, "-")
			if len(numStrs) == 1 {
				nodelist = append(nodelist, nodeRootName+numStrs[0])
				continue
			}
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
			for i := lowInd; i < highInd+1; i++ {
				nodeName := nodeRootName + strconv.Itoa(i)
				nodelist = append(nodelist, nodeName)
			}
		}
	}

	ports := make([]string, len(nodelist))
	for i := range ports {
		ports[i] = ":" + strconv.Itoa(5000+i)
	}

	fullNodelist := ""
	for i := range nodelist {
		fullNodelist += nodelist[i] + ports[i]
		if i != len(nodelist)-1 {
			fullNodelist += ","
		}
	}

	//fmt.Println("nodelist = ", nodelist)
	//fmt.Println("fullNodelist = ", fullNodelist)

	wg := &sync.WaitGroup{}
	wg.Add(len(nodelist))
	for i := range nodelist {
		go func(i int) {
			defer wg.Done()
			args := []string{"-N", "1", "-n", "1", "-c", "12", "--nodelist", nodelist[i], os.Args[1]}
			for i := 2; i < len(os.Args); i++ {
				args = append(args, os.Args[i])
			}

			args = append(args, "-mpi-addr", nodelist[i]+ports[i], "-mpi-alladdr", fullNodelist)
			//fmt.Println("args = ", args)
			cmd := exec.Command("srun", args...)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			cmd.Run()
		}(i)
	}
	wg.Wait()
}
