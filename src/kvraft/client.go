package kvraft

import (
	"fmt"
	"labrpc"
	"sync"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	rpcId int64
	clientId int64
	mu      sync.Mutex
	leaderId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.rpcId = 0
	ck.clientId = nrand()
	ck.leaderId = 0
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.mu.Lock()
	args := GetArgs{}
	args.Key = key
	args.ClientId = ck.clientId
	ck.rpcId += 1
	args.RpcId = ck.rpcId
	reply := GetReply{}

	for {
		for i := 0; i < len(ck.servers); i++ {
			ok := ck.servers[i].Call("KVServer.Get", &args, &reply)

			fmt.Printf("len(ck.servers): %v call server[%v] ok: %v reply.Err: %v \n", len(ck.servers), i, ok, reply.Err)

			if ok && reply.Err == OK {
				ck.leaderId = i
				ck.mu.Unlock()
				return reply.Value
			}
			if ok && reply.Err == ErrNoKey {
				ck.mu.Unlock()
				return "-1"
			}
		}
	}

	ck.mu.Unlock()

	return "-2"
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	ck.rpcId += 1
	args.RpcId = ck.rpcId
	args.ClientId = ck.clientId
	reply := PutAppendReply{}
	//findLeader := true
	//initIndex := ck.leaderId
	for {

		//if !findLeader {
		//	initIndex = 0
		//	findLeader = false
		//}

		for i := 0; i < len(ck.servers); i++ {
			ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
			fmt.Printf("len(ck.servers): %v call server[%v] ok: %v reply.Err: %v \n",len(ck.servers), i, ok, reply.Err)
			if ok && reply.Err == OK {
				ck.leaderId = i
				ck.mu.Unlock()
				return
			}
			if ok && reply.Err == ErrNoKey {
				ck.mu.Unlock()
				return
			}
		}
	}

	ck.mu.Unlock()
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
