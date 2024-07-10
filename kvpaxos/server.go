package kvpaxos

import (
	//interfacestructures "cse-513/src/interface_structures"
	"cse-513/src/paxos"
	"encoding/gob"
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

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OperationType  int
	ClientId       int64
	SequenceNumber int64
	Key            string
	Value          string
}

const (
	OP_GET    = 0
	OP_PUT    = 1
	OP_APPEND = 2
)

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	LastSeq     map[int64]int64   // storing as a map to client id // reason  being to avoid duplicate client requests
	StoredValue map[string]string // this is the db of key/value stores
	LastApplied int               // the last sequence number to be applied
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	//call paxos start without op's value // it will return if the value's already decided in current node
	// and if not will get the value from other nodes.
	ClientID := args.ClientId
	ClientSeq := args.SeqNo
	// first check if the clientseq is less than last seq // in that case we received a out of order request
	// raise an error
	// case 2: already this seq is executed -> get the value and use it
	// case 3: execute it now and wait for agreement.
	kv.mu.Lock()
	if lastseq, exists := kv.LastSeq[ClientID]; exists {
		if ClientSeq < lastseq {
			panic(" Incorrect seq: out of order request received")
		}
		if ClientSeq == lastseq {
			//case 2: get the value and send it back from storedvalues.
			val := kv.StoredValue[args.Key]
			if val == "" {
				reply.Err = ErrNoKey
			} else {
				reply.Err = OK
				reply.Value = val
			}
			kv.mu.Unlock()
			return nil
		}
	}
	kv.mu.Unlock()
	InstanceSequenceNumber := kv.px.Max() + 1
	op := Op{
		OperationType:  OP_GET,
		ClientId:       ClientID,
		SequenceNumber: ClientSeq,
		Key:            args.Key,
	}
	//println(InstanceSequenceNumber)
	//println("starting paxos get for client %s and seq %s and key %s", args.ClientId, args.SeqNo, args.Key, kv.me)
	kv.px.Start(InstanceSequenceNumber, op)
	//println(kv.px.Max())
	for !kv.isdead() {
		stopper, success := kv.checkStatus(InstanceSequenceNumber, ClientID, ClientSeq)
		if !stopper {
			time.Sleep(2 * time.Millisecond)
			continue
		}
		if success == false {
			reply.Err = "ErrConflict"
		} else {
			reply.Err = OK
			kv.mu.Lock()
			//reply.Value = kv.StoredValue[args.Key]
			reply.Value = kv.StoredValue[args.Key]
			kv.mu.Unlock()
			//return nil
		}
		return nil
	}
	reply.Err = "ServerKilled"
	return nil
}

func (kv *KVPaxos) checkStatus(instanceseqno int, clientid int64, clientseqno int64) (bool, bool) {
	//println("check status started for ", kv.me)
	kv.mu.Lock()
	//println("lock acquired for check status ", kv.me)
	defer kv.mu.Unlock()
	if kv.LastApplied < instanceseqno {
		//println(" sleeping because of ", kv.LastApplied, instanceseqno, kv.me)
		return false, false
	}
	if lastSeq, ok := kv.LastSeq[clientid]; !ok || lastSeq != clientseqno {
		return true, false
	}
	//println("sucess ", kv.LastApplied, instanceseqno, kv.me)
	return true, true
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	ClientID := args.ClientId
	ClientSeq := args.SeqNo
	// first check if the clientseq is less than last seq // in that case we received a out of order request
	// raise an error
	// case 2: already this seq is executed -> get the value and use it
	// case 3: execute it now and wait for agreement.
	kv.mu.Lock()
	if lastseq, exists := kv.LastSeq[ClientID]; exists {
		if ClientSeq < lastseq {
			panic(" Incorrect seq: out of order request received")
		}
		if ClientSeq == lastseq {
			//case 2: get the value and send it back from storedvalues.
			reply.Err = OK
			kv.mu.Unlock()
			return nil
		}
	}
	kv.mu.Unlock()
	InstanceSequenceNumber := kv.px.Max() + 1
	op := Op{
		OperationType:  args.OperationType,
		ClientId:       ClientID,
		SequenceNumber: ClientSeq,
		Key:            args.Key,
		Value:          args.Value,
	}
	//println("starting paxos put for client %s and seq %s and key %s", args.ClientId, args.SeqNo, args.Key, kv.me, InstanceSequenceNumber)
	kv.px.Start(InstanceSequenceNumber, op)
	//temp := kv.px.Max()
	//println(temp)
	for !kv.isdead() {
		stopper, success := kv.checkStatus(InstanceSequenceNumber, ClientID, ClientSeq)
		if !stopper {
			//println("sleeping")
			time.Sleep(2 * time.Millisecond)
			continue
		}
		if !success {
			//println("error happened")
			reply.Err = "ErrSeqConflict"
		} else {
			reply.Err = OK
		}
		return nil
	}
	reply.Err = "ServerKilled"
	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me
	kv.LastSeq = make(map[int64]int64)
	kv.StoredValue = make(map[string]string)
	//kv.LastValue = make(map[int64]interface{})
	kv.LastApplied = -1

	// Your initialization code here.

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)
	go kv.concurrentLogApply()

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}

func (kv *KVPaxos) concurrentLogApply() {
	// / this function thread checks for agreed values and applies it
	// i.e it stores the value in stored_value table
	// sets lastsequence number for that client

	for !kv.isdead() {
		//kv.mu.Lock()
		maxSeqKnown := kv.px.Max()
		//kv.mu.Unlock()
		for i := kv.LastApplied + 1; i <= maxSeqKnown; i++ {
			// from last applied seq to current max. try to apply it to log if decided
			status, op := kv.px.Status(i)
			if status != paxos.Decided {
				// if not decided, wait for it to be decided
				// TODO: need to fill gap
				if i < maxSeqKnown {
					//	DPrintf("%s not decided try fill gap", prefix)
					kv.fillGap(i)
					status, op = kv.px.Status(i)
					if status != paxos.Decided {
						panic(fmt.Sprintf("seq %d not decided after fill gap", i))
					}
				} else {
					time.Sleep(2 * time.Millisecond)
					continue
				}
			}
			//println("going to lock for log apply in server ", kv.me)
			kv.mu.Lock()
			//println("lock acquired successfully for log apply for ", kv.me)
			Operation := op.(Op)

			lastseqnumber, exists := kv.LastSeq[Operation.ClientId]
			if !exists || lastseqnumber != Operation.SequenceNumber {
				//println("log applied for", Operation.SequenceNumber, kv.me)
				// if no entry exists or last client seq applied is not equal to current operation's seq
				// then apply this operation and store the value
				if Operation.SequenceNumber < lastseqnumber {
					//out of order execution of operations raise an error message
					panic("Out of order execution of operations : Fatal error")
				}
				if Operation.OperationType == OP_GET {
					//key := Operation.Key
					kv.LastSeq[Operation.ClientId] = Operation.SequenceNumber
				} else if Operation.OperationType == OP_PUT {
					key := Operation.Key
					value := Operation.Value
					kv.StoredValue[key] = value
					kv.LastSeq[Operation.ClientId] = Operation.SequenceNumber
					//println("for server", kv.me, " last put value applied is ", Operation.Value, " for seq ", Operation.SequenceNumber)
				} else if Operation.OperationType == OP_APPEND {
					key := Operation.Key
					value := Operation.Value
					kv.StoredValue[key] = kv.StoredValue[key] + value
					kv.LastSeq[Operation.ClientId] = Operation.SequenceNumber
				}
			}
			//println("for server", kv.me, " last applied is ", i)
			kv.LastApplied = i
			kv.mu.Unlock()
			kv.px.Done(i)
			kv.px.Min()
			//println("%s done %d min %d", Operation.Value, i, min)
		}
		time.Sleep(1 * time.Millisecond)
	}
}
// fillGap is a method of the KVPaxos struct for ensuring that a gap in the Paxos log at a given sequence number is filled.
func (kv *KVPaxos) fillGap(seq int) {

	op := Op{
		OperationType: 4, // we expect the value to be filled from other peers . this is just no-op
	}

	kv.px.Start(seq, op)
	wait_time := 10 * time.Millisecond
	// Keep checking the status until the operation at the specified sequence number is decided.
	for !kv.isdead() {
		decided, _ := kv.px.Status(seq)
		if decided == paxos.Decided {
			return
		}
		time.Sleep(wait_time)
		if wait_time < 5*time.Second {
			wait_time *= 2 // increasing wait time exponentially
		}
	}
}
