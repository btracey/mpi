package mpi

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"
)

// Network implements the MPI protocol using network calls provided by the net
// package in the standard library. Net creates an all-to-all connection using
// specified nework protocol among all provided IP addresses. Network uses
// encoding/gob for (de)serialization, and so some network protocols may not
// be appropriate. While (at present) Network is not built with security in mind,
// the network does confirm that the provided password is the
// same before accepting any connection. At some point, the Password will be put through
// a hash function, but that is not yet implemented
//
// Network uses the flags provided. It takes the values provided by the flags
// if the zero values are present for the network values.
type Network struct {
	NetProto string        // Which network protocol to use (see net package for options)
	Addr     string        // Network address of the local process
	Addrs    []string      // List of the addresses of all nodes. Addr must be among them
	Timeout  time.Duration // If set, Init fail if the connections are not made within the duration

	Password       string
	hashedPassword string

	myrank int // rank of this process
	nNodes int // total number of processes

	connections []*pairwiseConnection // connections to all of the other nodes
	local       *local
}

func (n *Network) Rank() int {
	if n.nNodes == 0 {
		return -1
	}
	return n.myrank
}

func (n *Network) Size() int {
	return n.nNodes
}

// Init implements the Mpi init function
func (n *Network) Init() error {
	n.setFields()
	if len(n.Addrs) == 0 {
		n.Addr = ":5000"
		n.Addrs = []string{":5000"}
	}
	err := n.assignRanks()
	if err != nil {
		return err
	}

	return n.startConnections()
}

// setFields sets the fields of the network struct from flags if they were not
// set with a value that was registered
func (n *Network) setFields() {
	if n.NetProto == "" {
		n.NetProto = FlagProtocol
	}
	if n.Password == "" {
		n.Password = FlagPassword
	}
	if n.Timeout == 0 {
		n.Timeout = time.Duration(FlagInitTimeout)
	}
	if n.Addr == "" {
		n.Addr = FlagAddr
	}
	if len(n.Addrs) == 0 {
		n.Addrs = make([]string, len(FlagAllAddrs))
		for i, str := range FlagAllAddrs {
			n.Addrs[i] = str
		}
	}
	n.hashedPassword = n.Password // TODO: Fix
}

// assignRanks gives each node in the network a unique rank in a way such that
// all processors will agree on the ranks of each node
func (n *Network) assignRanks() error {
	sort.Strings(n.Addrs)

	if !uniqueAddrs(n.Addrs) {
		return fmt.Errorf("network addresses not unique. list is: ", n.Addrs)
	}

	n.myrank = sort.SearchStrings(n.Addrs, n.Addr)

	if !(n.myrank < len(n.Addrs) && n.Addrs[n.myrank] == n.Addr) {
		return fmt.Errorf("mpi init: local ip address not in global list. Local address is: %v, global list is %v", n.myrank, n.Addrs)
	}

	n.nNodes = len(n.Addrs)
	return nil
}

func uniqueAddrs(addrs []string) bool {
	for i := 0; i < len(addrs)-1; i++ {
		if addrs[i] == addrs[i+1] {
			return false
		}
	}
	return true
}

// startConnections creates two-way all-to-all connections. Each node will
// dial all of the other nodes in the network and listen for their dials
func (n *Network) startConnections() error {
	n.connections = make([]*pairwiseConnection, n.nNodes)
	for i := range n.connections {
		n.connections[i] = &pairwiseConnection{
			receivetags: newTagManager(),
			sendtags:    newTagManager(),
		}
	}

	n.local = newLocal()

	var listenError error
	var dialError error

	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		listenError = n.establishListenConnections()
	}()

	go func() {
		defer wg.Done()
		dialError = n.establishDialConnections()
	}()

	wg.Wait()

	if listenError != nil {
		return listenError
	}

	if dialError != nil {
		return dialError
	}

	return nil
}

// establishListenConnections listens on the address of this node for dials
// from of the other nodes
func (n *Network) establishListenConnections() error {
	// Listen on the local IP address for calls from the other processes
	listener, err := net.Listen(n.NetProto, n.Addr)
	if err != nil {
		return errors.New("error listening: " + err.Error())
	}

	connErr := make([]error, n.nNodes)
	wg := &sync.WaitGroup{}

	wg.Add(n.nNodes - 1)
	for i := 0; i < n.nNodes; i++ {
		if i == n.myrank {
			continue // Don't listen to yourself
		}
		go func(i int) {
			defer wg.Done()
			connErr[i] = n.listenHandshake(listener)
		}(i)
	}
	wg.Wait()

	// Combine the errors if there were any
	var str string
	for _, err := range connErr {
		if err != nil {
			str += " " + err.Error()
		}
	}
	if str != "" {
		return errors.New(str)
	}
	return nil
}

type initialMessage struct {
	Password string // Password for
	Id       int    // Node
}

type listConn struct {
	conn net.Conn
	err  error
}

// listenHandshake accepts an incoming message from another node in the network
// and responds that the message was received. There is a timeout of the user
// requested one
func (n *Network) listenHandshake(listener net.Listener) error {

	acceptChan := make(chan listConn)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			err = errors.New("error accepting: " + err.Error())
		}
		acceptChan <- listConn{conn, err}
	}()

	var list listConn
	if n.Timeout > 0 {
		timer := time.NewTimer(n.Timeout)
		select {
		case list = <-acceptChan:
		case <-timer.C:
			list = listConn{
				err: errors.New("listener timed out"),
			}
		}
	} else {
		list = <-acceptChan
	}

	if list.err != nil {
		return list.err
	}

	conn := list.conn
	decoder := gob.NewDecoder(conn)

	var message initialMessage
	err := decoder.Decode(&message)
	if err != nil {
		return err
	}

	id, err := n.passwordAndId(message)
	if err != nil {
		return err
	}

	n.connections[id].listen = conn

	// Send back a handshake the other way
	return gob.NewEncoder(conn).Encode(
		initialMessage{
			Password: n.hashedPassword,
			Id:       n.myrank,
		})
}

func (n *Network) establishDialConnections() error {
	// Each program also dials every other program
	connErr := make([]error, n.nNodes)
	wg := &sync.WaitGroup{}
	wg.Add(n.nNodes - 1)
	for i := 0; i < n.nNodes; i++ {
		if i == n.myrank {
			continue // Don't dial yourself
		}
		go func(i int) {
			defer wg.Done()
			connErr[i] = n.dialHandshake(i)
		}(i)
	}
	wg.Wait()

	var str string
	for _, err := range connErr {
		if err != nil {
			str += " " + err.Error()
		}
	}
	if str != "" {
		return errors.New(str)
	}

	return nil
}

// dialHandshake dials the ith node in the network and waits for a confirmation
// message. If the node isn't immediately available, the code retries at regular
// intervals until timeout
func (n *Network) dialHandshake(i int) error {
	interval := 100 * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var conn net.Conn
	var err error
	start := time.Now()

	// range until connection is established or too much time has elapsed
	for _ = range ticker.C {
		conn, err = net.DialTimeout(n.NetProto, n.Addrs[i], n.Timeout)
		if err == nil || (n.Timeout > 0 && time.Since(start) > n.Timeout) {
			break
		}
	}
	if err != nil {
		return err
	}

	// Established the connection, send the first handshake message
	err = gob.NewEncoder(conn).Encode(initialMessage{
		Password: n.hashedPassword,
		Id:       n.myrank,
	})
	if err != nil {
		return err
	}

	// Recieve the handshake message back and verify the password matches
	var message initialMessage
	err = gob.NewDecoder(conn).Decode(&message)
	if err != nil {
		return err
	}

	id, err := n.passwordAndId(message)
	if err != nil {
		return err
	}
	n.connections[id].dial = conn
	return nil
}

// Checks that the password matches what the network expects and that the
// id is valid
func (n *Network) passwordAndId(message initialMessage) (int, error) {
	if message.Password != n.hashedPassword {
		return -1, errors.New("bad password")
	}
	if message.Id >= n.nNodes || message.Id < 0 || message.Id == n.myrank {
		return -1, fmt.Errorf("bad id: %v", message.Id)
	}
	return message.Id, nil
}

// Finalize implements mpi.Interface.Finalize
func (n *Network) Finalize() {
	n.close()
}

// close closes all of the connections
// TODO: Check that this is right
func (n *Network) close() {
	for _, conn := range n.connections {
		if conn.dial != nil {
			conn.dial.Close()
		}
		if conn.listen != nil {
			conn.listen.Close()
		}
	}
}

/*
	Issue with sending locally:
	In the distributed framework, we know exactly one of Send or Receive is
	called. If a tag is registered twice, it is a bug. This is not true locally.
	Send and receive are both be called exactly once in correct programs.
	Aditionally, in correct programs, send and receive are called concurrently
	and thus there are no guarantees about their order. Additionally, by the
	MPI spec, Send must wait until the receive is done.

	The implementation is as follows. Whichever of Send/Receive arrives first
	creates the communication channel and logs that it was the creator. It then
	establishes its half of the communication. Whichever arrives second observes
	that the channel is there and provides the second link for the communication.
	Finally, once the communication is complete, the sender will delete the entry
	from the map so that the tag can be reused.
*/

type localsend struct {
	c      chan []byte
	create creator
}

type creator int

const (
	notcreated = iota
	sendcreate
	receivecreate
)

type local struct {
	mux   *sync.Mutex
	sends map[int]localsend
}

func newLocal() *local {
	return &local{
		mux:   &sync.Mutex{},
		sends: make(map[int]localsend),
	}
}

func (l *local) Send(tag int, b []byte) {
	l.mux.Lock()
	s, ok := l.sends[tag]
	if ok && s.create == sendcreate {
		panic(fmt.Sprintf("tag %d created twice by sender locally", tag))
	}
	if !ok {
		l.sends[tag] = localsend{
			c:      make(chan []byte),
			create: sendcreate,
		}
	}
	l.mux.Unlock()
	l.sends[tag].c <- b
	l.mux.Lock()
	delete(l.sends, tag)
	l.mux.Unlock()
}

func (l *local) Receive(tag int) []byte {
	l.mux.Lock()
	s, ok := l.sends[tag]
	if ok && s.create == receivecreate {
		panic(fmt.Sprintf("tag %d created twice by receiver locally", tag))
	}
	if !ok {
		l.sends[tag] = localsend{
			c:      make(chan []byte),
			create: receivecreate,
		}
	}
	l.mux.Unlock()
	return <-l.sends[tag].c
}

// tagManager is used to manage tagged messages
type tagManager struct {
	CommMap map[int]chan []byte
	Mux     *sync.Mutex
}

func newTagManager() *tagManager {
	return &tagManager{
		CommMap: make(map[int]chan []byte),
		Mux:     &sync.Mutex{},
	}
}

// Register adds a tag to the list of used tags and creates a communication
// channel. This allows messages to be sent to the correct goroutines even though
// multiple sends may be happening on the same channel at the same time.
func (t *tagManager) Register(tag int) {
	t.Mux.Lock()
	defer t.Mux.Unlock()
	_, ok := t.CommMap[tag]
	if ok {
		panic(fmt.Sprintf("tag %d already exists on the connection", tag))
	}
	t.CommMap[tag] = make(chan []byte)
}

// Delete removes the tag from the map
func (t *tagManager) Delete(tag int) {
	t.Mux.Lock()
	defer t.Mux.Unlock()
	// TODO: Remove this once we're sure the implementation is correct
	_, ok := t.CommMap[tag]
	if !ok {
		panic("attempt to delete non-existant key")
	}
	delete(t.CommMap, tag)
}

// Channel returns the channel for that tag
func (t *tagManager) Channel(tag int) chan []byte {
	t.Mux.Lock()
	defer t.Mux.Unlock()
	// TODO: Remove this once we're sure the implementation is correct
	_, ok := t.CommMap[tag]
	if !ok {
		panic("attempt to return chan from non-existant tag")
	}
	c := t.CommMap[tag]
	return c
}

// a pairwiseConnection is a manager for the data going between two nodes in
// the network
type pairwiseConnection struct {
	dial        net.Conn    // Connection on which to send data
	listen      net.Conn    // Receive on which to receive data (and send confirmation message)
	receivetags *tagManager // tags on the send connection
	sendtags    *tagManager // tags on the receive connection
}

// message to send over the wire
// message is of type {int, []byte} so we know the type of the data
// when deserializing
type message struct {
	Tag   int
	Israw bool // flag if it's raw bytes
	Bytes []byte
}

// Send implements mpi.Interface.Send. Network uses the encoding/gob package to
// serialize data.
func (n *Network) Send(data interface{}, destination, tag int) error {

	/*
		Implementation comments:
		The mpi.Interface.Send specifies that sends between two nodes may happen
		concurrently as long as the tags are unique. These concurrent sends may
		have different data types, but the program must know the type of data in
		order to decode from the communication channel. The solution to this is
		to serialize all of the data into a []byte, and to only send the "message"
		type over the channel. Gob can decode the message, observe the tag, and
		pass along the []byte to receive for further decoding. When Registering
		the tag, the tagManager also creates a communication channel to do the
		forwarding once the tag is observed.
	*/
	manager := n.connections[destination].sendtags
	// register the tag for this message
	manager.Register(tag)

	// serialize the data into a []byte
	var buf bytes.Buffer

	err := gob.NewEncoder(&buf).Encode(data)
	if err != nil {
		return err
	}

	// special case if sending locally
	if destination == n.myrank {
		n.local.Send(tag, buf.Bytes())
		return nil
	}

	// Launch a reader for the reply message from the destination
	go func() {
		var m message
		err := gob.NewDecoder(n.connections[destination].dial).Decode(&m)
		if err != nil {
			panic(err) // There should never be a send over the connection that isn't a message
		}

		manager.Channel(m.Tag) <- m.Bytes
	}()

	// send the data over the connection.
	enc := gob.NewEncoder(n.connections[destination].dial)
	err = enc.Encode(message{Tag: tag, Bytes: buf.Bytes()})
	if err != nil {
		return err
	}

	// Wait for the confirmation message and then delete the tag
	<-manager.Channel(tag)
	manager.Delete(tag)
	return nil
}

// Receive implements the Mpi function. Receive uses gob to decode the data.
func (n *Network) Receive(data interface{}, source, tag int) error {
	// Receive adds the tag to the map, launches a goroutine to listen for the
	// reply, and deserializes the data when it comes

	manager := n.connections[source].receivetags

	var b []byte
	if source == n.myrank {
		b = n.local.Receive(tag)
	} else {
		manager.Register(tag)

		go n.receiveReader(source) // decoupled because there may be concurrent sends

		// Receive the bytes, delete the tag, and decode the bytes
		b = <-manager.Channel(tag)
		manager.Delete(tag)
	}

	buf := bytes.NewBuffer(b)
	decoder := gob.NewDecoder(buf)

	err := decoder.Decode(data)
	if err != nil {
		return err
	}
	return nil
}

// receiveReader reads from the connection and returns a data value to the
// appropriate tag
// TODO: remove panic
func (n *Network) receiveReader(source int) {
	var m message
	err := gob.NewDecoder(n.connections[source].listen).Decode(&m)
	if err != nil {
		panic(err)
	}

	n.connections[source].receivetags.Channel(m.Tag) <- m.Bytes

	// Send a confirmation message of receipt
	reply := message{
		Tag: m.Tag,
	}
	encoder := gob.NewEncoder(n.connections[source].listen)
	err = encoder.Encode(reply)
	if err != nil {
		panic(err)
	}
}
