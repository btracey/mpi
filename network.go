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
	local       *localConnection
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

	n.local = &localConnection{
		manager:    newTagManager(),
		storedData: make(map[int][]byte),
		mux:        &sync.Mutex{},
	}

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

// Finalize implements the Mpi init function
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

type localConnection struct {
	manager    *tagManager
	storedData map[int][]byte
	mux        *sync.Mutex
}

func (l *localConnection) AddBytes(tag int, b []byte) error {
	err := l.manager.Add(tag)
	if err != nil {
		return err
	}
	l.mux.Lock()
	l.storedData[tag] = b
	l.mux.Unlock()
	return nil
}

func (l *localConnection) Bytes(tag int) ([]byte, error) {
	l.mux.Lock()
	b, ok := l.storedData[tag]
	l.mux.Unlock()
	if !ok {
		return nil, errors.New("Unknown tag")
	}
	return b, nil
}

func (l *localConnection) Delete(tag int) {
	l.manager.Delete(tag)
	l.mux.Lock()
	delete(l.storedData, tag)
	l.mux.Unlock()
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

// adds a tag to the map, returning an error if the tag already exists. Uses
// a mutex to be thread safe
func (t *tagManager) Add(tag int) error {
	t.Mux.Lock()
	defer t.Mux.Unlock()
	_, ok := t.CommMap[tag]
	if ok {
		return TagExists{Tag: tag}
	}
	t.CommMap[tag] = make(chan []byte)
	return nil
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

type pairwiseConnection struct {
	dial        net.Conn // Send on
	listen      net.Conn // Receive from
	receivetags *tagManager
	sendtags    *tagManager
}

// message to send over the wire
type message struct {
	Tag   int
	Bytes []byte
}

type localMap map[int][]byte

// Send implements the Mpi function
func (n *Network) Send(data interface{}, destination, tag int) error {

	// Send serializes the data using gob, and then encodes the tag and the
	// bytes. message is not of type {int, interface{}} because then we couldn't
	// deserialize without knowing the type, which would make concurrent sends
	// impossible

	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(data)
	if err != nil {
		return err
	}

	if destination == n.myrank {
		n.local.AddBytes(tag, buf.Bytes())
		return nil
	}

	err = n.connections[destination].sendtags.Add(tag)
	if err != nil {
		return err
	}

	go n.confirmationReader(destination)

	enc := gob.NewEncoder(n.connections[destination].dial)
	err = enc.Encode(message{Tag: tag, Bytes: buf.Bytes()})
	if err != nil {
		return err
	}
	// Wait for confirmation that the send was received
	err = n.wait(destination, tag)
	if err != nil {
		return err
	}
	return nil
}

// launchConfirmationReader launches a goroutine that reads for the confirmation
// of a received message. When confirmation is received, it will send empty bytes
// on the channel of the correct goroutine
func (n *Network) confirmationReader(destination int) {
	var m message
	decoder := gob.NewDecoder(n.connections[destination].dial)
	err := decoder.Decode(&m)
	if err != nil {
		panic(err)
	}
	// Send a signal to the channel of this tag
	n.connections[destination].sendtags.Channel(m.Tag) <- m.Bytes
}

// wait waits for confirmation that the destination received send with the given tag
func (n *Network) wait(destination, tag int) error {
	// Wait for a receive from that tag, and then delete the tag to free it from
	// reuse
	if destination == n.myrank {
		<-n.local.manager.Channel(tag)
		n.local.Delete(tag)
		return nil
	}
	<-n.connections[destination].sendtags.Channel(tag)
	n.connections[destination].sendtags.Delete(tag)
	return nil
}

// Receive implements the Mpi function
func (n *Network) Receive(data interface{}, source, tag int) error {
	// Receive adds the tag to the map, launches a goroutine to listen for the
	// reply, and deserializes the data when it comes

	manager := n.connections[source].receivetags

	var b []byte
	if source == n.myrank {
		// Get the stored byte list and send a completion signal
		var err error
		b, err = n.local.Bytes(tag)
		if err != nil {
			panic(err)
		}
		go func(tag int) {
			n.local.manager.Channel(tag) <- []byte{}
		}(tag)
	} else {
		err := manager.Add(tag)
		if err != nil {
			return err
		}

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
func (n *Network) receiveReader(source int) {
	var m message
	decoder := gob.NewDecoder(n.connections[source].listen)
	err := decoder.Decode(&m)
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
