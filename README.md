# Paxos_consensus protocol with fault tolerant key value store

<b>Project Overview</b>
This project was developed as a part of CSE Distributed Systems course. This was done in 2 parts. 
Part 1: Implemented Paxos consensus protocol
Part 2: Implemented key values store on top of Part 1

<b>Technical details</b>
<b>Part 1:</b> 
Interfaces
px = paxos.Make(peers []string, me int) // constructor
px.Start(seq int, v interface{}) // start agreement on new instance
px.Status(seq int) (fate Fate, v interface{}) // get info about an instance
px.Done(seq int) // ok to forget all instances <= seq
px.Max() int // highest instance seq known, or -1
px.Min() int // instances before this have been forgotten

An application calls `Make(peers,me)` to create a Paxos peer, which will be used in your next assignment. You don't need to implement the part that calls `Make(peers, me)` in this assignment. The `peers` argument contains the ports of all the peers (including this one), and the `me` argument is the index of this peer in the peers array. 

`Start(seq,v)` asks Paxos to start agreement on instance seq, with proposed value v; `Start()` should return immediately, without waiting for agreement to complete. The application calls `Status(seq)` to find out whether the Paxos peer thinks the instance has reached agreement, and if so what the agreed value is. `Status()` should consult the local Paxos peer's state and return immediately; it should not communicate with other peers. The application may call `Status()` for old instances (but see the discussion of `Done()` below).

If application peers call `Start()` with different sequence numbers at about the same time, your implementation should run the Paxos protocol concurrently for all of them. You should not wait for agreement to complete for instance i before starting the protocol for instance i+1. Each instance should have its own separate execution of the Paxos protocol.

A long-running Paxos-based server must forget about instances that are no longer needed, and free the memory storing information about those instances. An instance is needed if the application still wants to be able to call `Status()` for that instance, or if another Paxos peer may not yet have reached agreement on that instance. Your Paxos should implement freeing of instances in the following way. When a particular peer application will no longer need to call `Status()` for any instance <= x, it should call `Done(x)`. That Paxos peer can't yet discard the instances, since some other Paxos peer might not yet have agreed to the instance. So each Paxos peer should tell each other peer the highest Done argument supplied by its local application. Each Paxos peer will then have a Done value from each other peer. It should find the minimum, and discard all instances with sequence numbers <= that minimum. The `Min()` method returns this minimum sequence number plus one.

Pseudo-code:

proposer(v):
    while not decided:
        choose n, unique and higher than any n seen so far
        send prepare(n) to all servers including self
        if prepare_ok(n, n_a, v_a) from majority:
            v' = v_a with highest n_a; choose own v otherwise
            send accept(n, v') to all
            if accept_ok(n) from majority:
                send decided(v') to all

acceptor's state:
    n_p (highest prepare seen)
    n_a, v_a (highest accept seen)

acceptor's prepare(n) handler:
    if n > n_p
        n_p = n
        reply prepare_ok(n, n_a, v_a)
    else
        reply prepare_reject

acceptor's accept(n, v) handler:
    if n >= n_p
        n_p = n
        n_a = n
        v_a = v
        reply accept_ok(n)
    else
        reply accept_reject


<b>Part 2:M/b> 
The key-value store includes three kinds of operations: Put, Get, and Append.
Append performs the same as Put when the key is not in the store.
Otherwise, it appends new value to the existing value. For example,
1. Put('k', 'a')
2. Append('k', 'bc')
3. Get(k) -> 'abc'

Clients send Put(), Append(), and Get() RPCs to kvpaxos servers. A client can send an RPC to any of the kvpaxos servers, and should retry by sending to a different server if there's a failure. Each kvpaxos server contains a replica of the key/value database; handlers for client Get() and Put()/Append() RPCs; and a Paxos peer. Paxos takes the form of a library that is included in each kvpaxos server. A kvpaxos server talks to its local Paxos peer (**via method calls**). All kvpaxos replicas should stay identical; the only exception is that some replicas may lag others if they are not reachable. If a replica isn't reachable for a while, but then starts being reachable, it should eventually catch up ( learn about operations that it missed).
