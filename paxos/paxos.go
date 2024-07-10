package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

var minseq_number int

type PaxosInstance struct {
	SequenceNumber        int   // store the sequence number of all instances current peer is working
	HighestProposalNumber int32 // proposal number used for this particular sequence
	state                 Fate  // fate of those peers
	Value                 interface{}
}

type PrepareArg struct {
	CallerId       int
	SequenceNumber int
	ProposalNumber int32
	MaximumDone    int
}

type PrepareReply struct {
	SequenceNumber int  // mandatory
	State          bool // mandatory true representing Promise and false representing reject response
	// in case of reject, we expect the highest proposal number and/or value which has been accepted
	Writer               bool
	MaximumDone          int
	HighesProposalNumber int         // conditional param
	AcceptedValue        interface{} // conditional param
}

type AcceptArg struct {
	CallerId       int
	SequenceNumber int
	ProposalNumber int32
	MaximumDone    int
	Value          interface{}
}

type AcceptReply struct {
	Acceptance_status bool // true - accepted false - rejected
	MaximumDone       int  // sequence number to be piggybacked in reply
	Writer            bool
}

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type Paxos struct {
	mu             sync.Mutex
	l              net.Listener
	dead           int32                 // for testing
	unreliable     int32                 // for testing
	rpcCount       int32                 // for testing
	peers          []string              // stores the port numbers of peers
	instances      map[int]PaxosInstance // this is supposed to store the information about states of all sequences current peer is working on
	me             int                   // index into peers[]
	MaximumDone    map[int]int
	ProposalNumber int32 // global no. representing the proposal number starting from 0 for all seq.
	// Your data here.
}

// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			//fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	//fmt.Println(err)
	return false
}

func (px *Paxos) updateMaxDone(id int, maxDone int) {
	px.mu.Lock()
	if maxDone > px.MaximumDone[id] {
		px.MaximumDone[id] = maxDone
	}
	px.mu.Unlock()
}

func (px *Paxos) Prepare(arg PrepareArg, reply *PrepareReply) error {
	// functions: check current highest known proposal number for this sequence
	// check with my current proposal number and if not highest reject
	// case 1: no entry exists in interface. add this entry
	// case 2: entry exists and not accepted - replace proposal number and send promise
	// case 3: entry exists and acceptred - send accept value and reject
	px.mu.Lock()
	if arg.ProposalNumber > px.ProposalNumber {
		if px.instances[arg.SequenceNumber].SequenceNumber == 0 {
			//case 1
			// value not being sent during prepase phase so just set it to 0
			px.instances[arg.SequenceNumber] = PaxosInstance{arg.SequenceNumber, arg.ProposalNumber, Pending, 0}
			reply.State = true
			reply.Writer = true
		} else {
			if px.instances[arg.SequenceNumber].state != Decided {
				// case 2
				px.instances[arg.SequenceNumber] = PaxosInstance{arg.SequenceNumber, arg.ProposalNumber, Pending, 0}
				reply.State = true
				reply.Writer = true
			} else {
				// case 3
				reply.State = false
				reply.SequenceNumber = arg.SequenceNumber
				reply.HighesProposalNumber = int(px.instances[arg.SequenceNumber].HighestProposalNumber)
				reply.AcceptedValue = px.instances[arg.SequenceNumber].Value
			}
		}
	} else {
		reply.State = false
		reply.SequenceNumber = arg.SequenceNumber
		reply.HighesProposalNumber = int(arg.ProposalNumber)
		reply.AcceptedValue = px.instances[arg.SequenceNumber].Value
		reply.Writer = true
	}
	reply.Writer = true
	px.mu.Unlock()
	reply.MaximumDone = px.MaximumDone[px.me]
	px.updateMaxDone(arg.CallerId, arg.MaximumDone)
	return nil
}

func (px *Paxos) Accept(arg AcceptArg, reply *AcceptReply) error {
	// functions: when accept is sent, we accept it if it is the highest number known
	px.mu.Lock()
	if arg.ProposalNumber >= px.instances[arg.SequenceNumber].HighestProposalNumber {
		px.instances[arg.SequenceNumber] = PaxosInstance{arg.SequenceNumber, arg.ProposalNumber, Decided, arg.Value}
		px.mu.Unlock()
		reply.Acceptance_status = true
		//reply.Minimum_seq = px.Min()
		reply.Writer = true
	} else {
		px.mu.Unlock()
		reply.Acceptance_status = false
		reply.Writer = true
	}

	reply.Writer = true
	reply.MaximumDone = px.MaximumDone[px.me]
	px.updateMaxDone(arg.CallerId, arg.MaximumDone)
	return nil
}

// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
func (px *Paxos) Start(seq int, v interface{}) {
	//ensures concurrent execution of multiple sequences for a single peer in paxos
	if seq >= px.Min() && px.instances[seq].state != Decided {
		go px.ConcurrentStart(seq, v)
	}
}

func (px *Paxos) PreparePhase(seq int, v *interface{}) (int, int) {
	// new implementation for prepare
	// create a promised table
	// now go on infinite loop until you receive promise_count majority
	var visited [100]bool
	if len(px.peers) > 100 {
		//safety condition
		fmt.Println("Array length exceeded")
		return -1, -1
	}
	// now run prepare phase
	var promise_count = 0
	var min_seq = -1
	for promise_count <= len(px.peers)/2 {
		peer_left := false
		for i, peerPort := range px.peers {
			//fmt.Println("entered here")
			if visited[i] == true {
				// when you already visited this guy and got response back, do nothing
			} else if i == px.me {
				// just add 1 to accepted count since I am going to accept my prepare
				promise_count++
				px.mu.Lock()
				px.rpcCount++
				px.mu.Unlock()
				visited[i] = true
			} else {
				peer_left = true
				// now make a call to the peer with this proposal
				//TO DO: proposal number uniqueness not handled yet
				var prepareReply PrepareReply
				var stopper = false
				var times = 0
				for {
					times++
					px.rpcCount++
					if (call(peerPort, "Paxos.Prepare", &PrepareArg{px.me, seq, px.ProposalNumber, px.MaximumDone[px.me]}, &prepareReply)) {
						px.updateMaxDone(i, prepareReply.MaximumDone)
						if prepareReply.Writer == false {
							fmt.Println("Flag not set")
							break
						}
						//fmt.Println(prepareReply.Tester)
						visited[i] = true
						if prepareReply.State {
							//peer sent us a promise message
							promise_count++
							break
							//fmt.Println(" got accepted ", px.ProposalNumber)
						} else {
							//TO DO handle the case when peer rejects it
							// check if there is a value sent
							if prepareReply.AcceptedValue != 0 {
								//peer sends accepted value when someone has already finished accepted phase
								// take the same value and proceed with accept phase
								*v = prepareReply.AcceptedValue
								stopper = true
								promise_count++
								break
							} else {
								px.mu.Lock()
								//accepted value not sent, so we can try with higher proposal number
								if prepareReply.HighesProposalNumber != 0 {
									px.ProposalNumber = int32(prepareReply.HighesProposalNumber) + int32(px.me) + 1
								} else {
									px.ProposalNumber++
								}
								px.mu.Unlock()
							}
						}
					} else {
						//TO DO : case when call fails but no gonna be handled now // so ignoring this  branch
						//TO DO : check if this requirement exist and remove the comment.
						break
					}
					if stopper || px.isdead() {
						break
					}

				}
			}
		}
		if peer_left == false {
			break
		}
		if promise_count <= len(px.peers)/2 {
			//fmt.Println("call failed without reaching majority: going to sleep")
			time.Sleep(1000 * time.Millisecond)
		}
	}
	if promise_count == len(px.peers) {
		return promise_count, min_seq
	} else {
		return promise_count, -1
	}
}

func (px *Paxos) AcceptPhase(seq int, v interface{}, min_seq int) int {
	var peer_left = false
	var visited [100]bool
	var acceptance_count = 0
	for acceptance_count <= len(px.peers)/2 {
		var stopper = false
		for i, peerPort := range px.peers {
			if visited[i] == true {

			} else if i == px.me {
				visited[i] = true
				px.mu.Lock()
				instance, _ := px.instances[seq]
				instance.state = Decided
				instance.Value = v
				px.instances[seq] = instance
				acceptance_count++
				px.mu.Unlock()
				px.Done(min_seq)
			} else {
				peer_left = true
				px.rpcCount++
				var acceptReply AcceptReply
				if (call(peerPort, "Paxos.Accept", &AcceptArg{px.me, seq, px.ProposalNumber, px.MaximumDone[px.me], v}, &acceptReply)) {
					px.updateMaxDone(i, acceptReply.MaximumDone)
					if acceptReply.Writer == false {
						fmt.Println("Flag not set")
						break
					}
					visited[i] = true
					if acceptReply.Acceptance_status {
						acceptance_count++
					} else {
						//someone else has started the process and got it accepted so stop this process
						// that proposer will take care of sending values to everyone
						stopper = true
					}
				} else {

					// not sure what to do
					//fmt.Println("Call failed during Accept Phase")
				}
			}
		}
		if stopper == true {
			break
		}
		if peer_left == false {
			break
		}
		if acceptance_count <= len(px.peers)/2 {
			//fmt.Println("call failed without reaching majority in accept phase: going to sleep")
			time.Sleep(1000 * time.Millisecond)
		}
	}
	return acceptance_count
}

func (px *Paxos) ConcurrentStart(seq int, v interface{}) {
	// Your code here.
	// lock paxos object
	//steps
	//1. Prepare phase - send prepare to all peers and get back data
	//2. if rejected, start again
	//3. if majority, send accept
	//4. if rejected, start again
	//5. if accepted, store it and idk what to do next.
	// TO DO: multithreading: each start should be running in a different peer so that we can start paxos for multiple
	// 		  sequences at the same time
	// info : sequences number refers to the location we are trying to put the value in - this is the current understanding I have now.

	//each start phase corresponds to a sequence number. so we add a entry to our known instances structure as well
	// always start with Pending state
	px.mu.Lock()
	px.instances[seq] = PaxosInstance{seq, px.ProposalNumber, Pending, v}
	// Prepare phaseee
	//promise_count := 0
	px.ProposalNumber = int32(int(px.ProposalNumber) + px.me + 1)
	px.mu.Unlock()
	//call prepare method
	_, min_seq := px.PreparePhase(seq, &v)
	//fmt.Println(promise_count)
	px.AcceptPhase(seq, v, min_seq)
	//fmt.Println(acceptance_count)
	//fmt.Println(px.peers[0])
	//fmt.Println(px.ProposalNumber)
	//px.ProposalNumber = px.ProposalNumber + int32(px.me)
	//fmt.Println(px.ProposalNumber)
	/*if px.isunreliable() {
		px.mu.Lock()
		instance, _ := px.instances[seq]
		instance.state = Decided
		instance.HighestProposalNumber = px.ProposalNumber
		instance.SequenceNumber = seq
		instance.Value = v
		px.instances[seq] = instance
		px.mu.Unlock()
		return
	}*/
}

// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
func (px *Paxos) Done(seq int) {
	//px.mu.Lock()
	//defer px.mu.Unlock() // automatically unlocks the object on the end of this function
	// Your code here.
	px.mu.Lock()
	if seq > px.MaximumDone[px.me] {
		px.MaximumDone[px.me] = seq
	}
	px.mu.Unlock()
	/*for i := 1; i <= seq; i++ {
		px.mu.Lock()
		instance, _ := px.instances[i]
		instance.state = Forgotten
		px.instances[i] = instance
		px.mu.Unlock()
	}*/
}

// the application wants to know the
// highest instance sequence known to
// this peer.
func (px *Paxos) Max() int {
	// Your code here.
	//basic understanding: needs to check all peers to find the working sequence number and returns the max among them
	// why and where it is used - still dont know

	var maxSeq int = -1
	for _, peer := range px.instances {
		if peer.SequenceNumber > maxSeq {
			maxSeq = peer.SequenceNumber
		}
	}

	return int(maxSeq)
}

// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
func (px *Paxos) Min() int {
	//px.mu.Lock()
	//defer px.mu.Unlock() // automatically unlocks the object on the end of this function
	// You code here.
	//var i = 0
	//var j = 0
	//var min_seq = -1 // initially when done is not called yet

	px.mu.Lock()
	//loop over maxdone map and find the min among everyone
	var min_done = px.MaximumDone[px.me]
	for i, _ := range px.peers {
		if px.MaximumDone[i] < min_done {
			min_done = px.MaximumDone[i]
		}
	}
	for i := range px.instances {
		if i <= min_done {
			instance := px.instances[i]
			instance.state = Forgotten
			instance.Value = nil
			px.instances[i] = instance
			//delete(px.instances, i)
		}
	}
	px.mu.Unlock()
	return min_done + 1
}

// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	//px.mu.Lock()
	//defer px.mu.Unlock() // automatically unlocks the object on the end of this function
	// Your code here.
	// px contains the instances it knows: loop over it and find the instance with seq number given and return status
	px.mu.Lock()
	defer px.mu.Unlock()
	instance, exists := px.instances[seq]
	if exists == true && instance.state == Decided {
		return Decided, instance.Value
	}
	return instance.state, nil
}

// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

// has this peer been asked to shut down?
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me
	px.instances = map[int]PaxosInstance{}
	px.MaximumDone = map[int]int{}
	for i, _ := range px.peers {

		px.MaximumDone[i] = -1
	}
	// Your initialization code here.

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 10 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 20 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
