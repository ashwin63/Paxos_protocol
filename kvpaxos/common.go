package kvpaxos

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	OperationType int
	ClientId      int64
	SeqNo         int64 // using this to identify unique requests
	Key           string
	Value         string
	Op            string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	ClientId int64
	SeqNo    int64 // using this to identify unique requests
	Key      string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
