# go-raft
### An Implementation of the Raft Consensus Algorithm in Go
This is a distributed consensus algorithm based on the [extended Raft paper](https://raft.github.io/raft.pdf). The Raft algorithm allows applications to replicate an append-only log between several servers. The main mechanism for keeping state replicated is leader election. One server is elected leader, and it sends entries to several follower servers. The leader will not respond to client requests unless the request has been safely replicated on a majority of followers. This allows for increased fault tolerance and availability. For more information, refer to the raft paper and [the raft website](https://raft.github.io/). 

To run the tests:
```
cd src
go test
```
