package kvpaxos

import (
	"crypto/rand"
	"math/big"
	"net/rpc"
	"time"
)

type Clerk struct {
	servers []string
	// You will have to modify this struct.
	me  int64
	seq int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.me = nrand()
	ck.seq = 0
	return ck
}

// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	//fmt.Println(err)
	return false
}

// Get is a method of the Clerk struct for retrieving the value associated with a key
// from a distributed key-value store using RPC calls to multiple servers.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.seq += 1
	// Create arguments for the RPC call.
	args := GetArgs{
		Key:      key,
		ClientId: ck.me,
		SeqNo:    ck.seq,
	}
	// Retry the RPC call until it succeeds or a timeout occurs.
	for {
		// Iterate through the servers and attempt the RPC call.
		for _, server := range ck.servers {
			reply := GetReply{}
			//prefix := fmt.Sprintf("get client %d call server %d with %v", ck.me, idx, args)
			ok := call(server, "KVPaxos.Get", &args, &reply)
			// Check if the RPC call was successful and the server's reply indicates success.
			if ok && reply.Err == OK {
				//DPrintf("%s ok with value %d", prefix, len(reply.Value))
				return reply.Value
			}
			// Check if the RPC call was successful and the server's reply indicates
            		// that the key is not present in the key-value store.
			if ok && reply.Err == ErrNoKey {
				//DPrintf("%s no key", prefix)
				return ""
			}
			// If the RPC call fails or the server's reply indicates an error, retry.
			//DPrintf("%s fail %t, %v", prefix, ok, reply)
		}
		time.Sleep(7 * time.Millisecond)
	}
}

// shared by Put and Append.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	// Determine the operation type (PUT or APPEND) based on the input string.
	var op_type int
	if op == "PUT" {
		op_type = OP_PUT
	} else {
		op_type = OP_APPEND
	}
	// Increment the sequence number.
	ck.seq += 1
	// Create arguments for the RPC call.
	args := PutAppendArgs{
		Key:           key,
		OperationType: op_type,
		Value:         value,
		ClientId:      ck.me,
		SeqNo:         ck.seq,
	}
	// Retry the RPC call until it succeeds or a timeout occurs.
	for {
		// Iterate through the servers and attempt the RPC call.
		for _, server := range ck.servers {
			reply := PutAppendReply{}
			//prefix := fmt.Sprintf("put client %d call server %d with %v", ck.me, idx, copy)
			// Perform the RPC call to the "KVPaxos.PutAppend" method.
			ok := call(server, "KVPaxos.PutAppend", &args, &reply)
			// Check if the RPC call was successful and the server's reply indicates success.
			if ok && reply.Err == OK {
				//	DPrintf("%s ok", prefix)
				//return reply.PreviousValue
				// Return from the function if the operation is successful.
				return
			}
			//DPrintf("%s fail %t, %v", prefix, ok, reply)
			// If the RPC call fails or the server's reply indicates an error, retry.
		}
		// If all servers fail to respond successfully, sleep for a short duration before retrying.
		time.Sleep(7 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "PUT")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "APPEND")
}
