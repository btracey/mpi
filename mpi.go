// Package mpi implements an mpi-like interface for go. This package seeks to
// enable distributed-memory parallel computation using only native go code.
// While this package seeks to present a familiar interface to users of MPI,
// it does not follow the MPI standard exactly. In cases where package
// documentation disagrees with the MPI standard, the package documentation
// should be considered correct.
//
// The Message Passing Interface, MPI [1], is a communications protocol for
// distributed memory systems. In MPI, a single program is executed in parallel
// on different machines. The MPI routines are used in order to communicate
// data between them. MPI emphasises speed over robustness, and should only be
// used in highly reliable systems, such as a computation cluster. From the
// standard: "An MPI implementation cannot or may choose not to handle some
// errors that occur during MPI calls".
//
// This package provides a set of MPI functions and defines the Mpi interface.
// Programs should use the function calls directly to allow portibility
// of programs. Generally, a specific implementation of mpi.Mpi should be
// regestered during an init() function of package main. Many of the functions
// allow errors to be returned, but it is up to the implementation to actually
// use the errors. Implementations may panic when errors occur.
//
// This package also provides mpi.Network, an implementation of the mpi.Mpi
// the interface built upon the net package in the standard library.
//
// A MPI program must begin with a call to Init() and should end with a call
// to Finalize(). Init determines the size, or number of nodes, size, to be used
// during the computation, and assigns each node a unique integer identifier,
// "rank", which has a value 0 <= rank < size. Init establishes connections
// among the nodes to allow point-to-point communication.
//
// During program exectution, processes may communicate with one another
// using the Send and Receive calls. MPI only specifies the behavior of routines,
// not their implementation
//
// Package MPI also adds several flags to aid in simplicity.
//		-mpi-addr : address of the local running process
//		-mpi-alladdr: comma separated list of the strings of all the addresses
//		-mpi-initimeout: time.Duration for how long init can take before timing out.
//		-mpi-protocol: string to represent the protocol to use
//		-mpi-password: password to use at MPI initialization
// Specific implementations are free to use or ignore these as desired
// flag.Parse() must be called in order to use these flags.
//
// By default, the Network protocol is used. See type documentation for behavior.
//
//
// [1] http://www.mcs.anl.gov/research/projects/mpi/
// [2] http://www.mpi-forum.org/docs/mpi-3.0/mpi30-report.pdf
package mpi

import "fmt"

var mpier Mpi = &Network{}
var registerCalled bool

// Register sets an Mpi implementation to be used in calls to MPI. Register
// should normally be called during program initialization and not again.
func Register(mpi Mpi) {
	// TODO: Provide check that Register is called no more than once
	mpier = mpi
}

// Init initializes the communication network. Init must be called before any
// other functions are called, and should only be called once during program
// execution
func Init() error {
	return mpier.Init()
}

// Finalize cleans up the commication network. After a call to finalize, no more
// Mpi calls may be made (though programs are free to continue execution)
func Finalize() {
	mpier.Finalize()
}

// Rank returns the rank of the local process. Each process has a unique rank
// in the network, and so the actual call to Rank will return a unique value for
// each machine. However, the rank of each process is agreed upon by all processes.
// The value of rank will not change during program execution. 0 <= Rank() < Size().
// As a special case, if the size of the network is zero (for example MPI is turned
// off or Init was not called), Rank returns -1
func Rank() int {
	return mpier.Rank()
}

// Size returns the total number of nodes. Size returns 0 if MPI is not initialized
func Size() int {
	return mpier.Size()
}

// Send transmits the data to the destination node with the given tag. Send may
// be called concurrently between any number of goroutines, but {destination, tag}
// pairs must be unique among concurrent calls to send.
// Send blocks until the data has been sent on connection, (thus data is again
// free to be modified), but does not wait for confirmation of receiving of the
// data. Wait may be used to do this. Once a call to Wait has completed, a
// {destination, tag} pair may be reused. A process may send to itself.
func Send(data interface{}, destination, tag int) error {
	return mpier.Send(data, destination, tag)
}

// Wait blocks until confirmation from destination that the data sent with the
// given tag has been received. Wait also frees the {destination, tag} pair for
// re-use.
func Wait(destination, tag int) error {
	return mpier.Wait(destination, tag)
}

// Receive reads from the connection with source and deserializes the bytes into
// data. Data should have the same type as send via send. Receive returns when
// the data has been deserialized.
func Receive(data interface{}, source, tag int) error {
	return mpier.Receive(data, source, tag)
}

// Mpi is a set of routines for performing parallel computation. See the
// function descriptions for documentation.
type Mpi interface {
	Init() error
	Finalize()
	Rank() int
	Size() int
	Send(data interface{}, destination, tag int) error
	Wait(destination, tag int) error
	Receive(data interface{}, source, tag int) error
}

// TagExists is an error type indicating the tag already has a concurrent request
// between the destination and source node
type TagExists struct {
	Tag int
	//SendRank    int
	//ReceiveRank int
}

func (t TagExists) Error() string {
	return fmt.Sprintf("Tag %v already in use sending", t.Tag)
}
