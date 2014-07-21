/*
Launches MPI tasks within a slurm environment. To use, first allocate nodes with
salloc, and then call
gompirunslurm ncores programname otherargs. For example,
salloc -N6 -c12
gompirunslurm 12 helloworld

Note that this syntax differs than that for gompirun. Number of cores here is the
number of cores per distributed process (not the number of processes).

gompirunslurm uses srun to launch the scripts within the allocation, one per
allocated node.
*/
package main

import (
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
)

func main() {
	if len(os.Args) < 3 {
		log.Fatal("mpirun_slurm must be called with the number of cores and the program name")
	}
	nCores, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	if nCores < 1 {
		log.Fatal("Must have more than one core")
	}
	programName := os.Args[2]

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

	wg := &sync.WaitGroup{}
	wg.Add(len(nodelist))
	for i := range nodelist {
		go func(i int) {
			defer wg.Done()
			args := []string{"-N", "1", "-n", "1", "-c", strconv.Itoa(nCores), "--nodelist", nodelist[i], programName}
			for i := 3; i < len(os.Args); i++ {
				args = append(args, os.Args[i])
			}

			args = append(args, "-mpi-addr", nodelist[i]+ports[i], "-mpi-alladdr", fullNodelist)
			cmd := exec.Command("srun", args...)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			cmd.Run()
		}(i)
	}
	wg.Wait()
}
