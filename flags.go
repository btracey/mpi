package mpi

import (
	"flag"
	"fmt"
	"strings"
	"time"
)

var FlagAddr string
var FlagAllAddrs AddrsFlag
var FlagInitTimeout DurationFlag
var FlagProtocol string
var FlagPassword string

type AddrsFlag []string

func (m *AddrsFlag) String() string {
	return fmt.Sprint(*m)
}

func (m *AddrsFlag) Set(value string) error {
	for _, str := range strings.Split(value, ",") {
		*m = append(*m, str)
	}
	return nil
}

type DurationFlag time.Duration

func (m *DurationFlag) String() string {
	return time.Duration(*m).String()
}

func (m *DurationFlag) Set(value string) error {
	dur, err := time.ParseDuration(value)
	if err != nil {
		return err
	}
	*m = DurationFlag(dur)
	return nil
}

func init() {
	flag.StringVar(&FlagAddr, "mpi-addr", "", "address of the local running process")
	flag.Var(&FlagAllAddrs, "mpi-alladdr", "addresses of all of the processes as comma separated values")
	flag.Var(&FlagInitTimeout, "mpi-inittimeout", "duration to wait before timeout in init")
	flag.StringVar(&FlagProtocol, "mpi-protocol", "tcp", "communication protocol to use")
	flag.StringVar(&FlagPassword, "mpi-password", "", "value to use for salting the mpi connection")
}
