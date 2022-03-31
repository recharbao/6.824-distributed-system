package kvraft

import (
	"bytes"
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType string
	Key string
	Value string
	ClientId int64
	RpcId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	lastApplyIndex int

	kvDatabase map[string]string

	clientRpc map[int64]int64
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	fmt.Printf("before lock Get=========================================================== server[%v]: kv.lastApplyIndex: %v  args.ClientId: %v args.RpcId: %v \n", kv.me, kv.lastApplyIndex, args.ClientId, args.RpcId)
	kv.mu.Lock()
	if args.Key == "" {
		reply.Err = ErrNoKey
		kv.mu.Unlock()
		return
	}
	fmt.Printf("before Get=========================================================== server[%v]: kv.lastApplyIndex: %v  args.ClientId: %v args.RpcId: %v \n", kv.me, kv.lastApplyIndex, args.ClientId, args.RpcId)

	rpcId, ok := kv.clientRpc[args.ClientId]
	if ok && args.RpcId == rpcId {
		reply.Err = OK
		reply.Value, ok = kv.kvDatabase[args.Key]
		kv.mu.Unlock()
		return
	}

	fmt.Printf("Get=========================================================== server[%v]: kv.lastApplyIndex: %v  args.ClientId: %v args.RpcId: %v \n", kv.me, kv.lastApplyIndex, args.ClientId, args.RpcId)
	op := Op{"Get", args.Key, "", args.ClientId, args.RpcId}
	entryIndex, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	t := 0
	for  kv.lastApplyIndex < entryIndex {
		fmt.Printf("Get1=========================================================== server[%v]:  kv.lastApplyIndex: %v entryIndex: %v \n", kv.me, kv.lastApplyIndex, entryIndex)
		time.Sleep(50 * time.Millisecond)
		t += 1
		if t > 10 {
			reply.Err = TooLongTime
			kv.mu.Unlock()
			return
		}
		_, isLeader1 := kv.rf.GetState()
		if !isLeader1 {
			reply.Err = ErrWrongLeaderFor
			kv.mu.Unlock()
			return
		}
	}

	reply.Value, ok = kv.kvDatabase[args.Key]

	// kv.clientRpc[args.ClientId] = args.RpcId

	reply.Err = OK
	kv.mu.Unlock()
}

func(kv *KVServer) ServerApply() {
	for {
		applyMsg := <-kv.applyCh
		if applyMsg.SnapshotValid {
			fmt.Printf("applyMsg.SnapshotIndex: %v \n", applyMsg.SnapshotIndex)
			if kv.rf.CondInstallSnapshot(applyMsg.SnapshotTerm, applyMsg.SnapshotIndex, applyMsg.Snapshot) {
				r := bytes.NewBuffer(applyMsg.Snapshot)
				d := labgob.NewDecoder(r)
				if d.Decode(&kv.lastApplyIndex) != nil || d.Decode(&kv.kvDatabase) != nil {
					fmt.Printf("readSnapshot error ! \n")
				}
				kv.lastApplyIndex = applyMsg.SnapshotIndex
			}
		} else if applyMsg.CommandValid {
			keyValue := applyMsg.Command.(Op)
			// fmt.Printf("rpcId: %v \n", keyValue.rpcId)
			rpcId, ok := kv.clientRpc[keyValue.ClientId]
			fmt.Printf("applyMsg.CommandIndex: %v   kv.clientRpc: %v  keyValue.clientId:  %v keyValue.rpcId: %v keyvalue.key: %v  keyValue.Value: %v \n",applyMsg.CommandIndex, kv.clientRpc, keyValue.ClientId, keyValue.RpcId, keyValue.Key, keyValue.Value)
			if ok && rpcId == keyValue.RpcId {
				kv.lastApplyIndex = applyMsg.CommandIndex
				continue
			}

			if keyValue.OpType == "Append" {
				s := kv.kvDatabase[keyValue.Key]
				s += keyValue.Value
				kv.kvDatabase[keyValue.Key] = s
			} else if keyValue.OpType == "Put" {
				kv.kvDatabase[keyValue.Key] = keyValue.Value
			}

			kv.lastApplyIndex = applyMsg.CommandIndex
			kv.clientRpc[keyValue.ClientId] = keyValue.RpcId
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	fmt.Printf("before lock put=========================================================== server[%v]: kv.lastApplyIndex: %v  args.ClientId: %v args.RpcId: %v \n", kv.me, kv.lastApplyIndex, args.ClientId, args.RpcId)
	kv.mu.Lock()
	if args.Key == "" {
		reply.Err = ErrNoKey
		kv.mu.Unlock()
		return
	}

	fmt.Printf("before put=========================================================== server[%v]: kv.lastApplyIndex: %v  args.ClientId: %v args.RpcId: %v \n", kv.me, kv.lastApplyIndex, args.ClientId, args.RpcId)
	rpcId, ok := kv.clientRpc[args.ClientId]
	if ok && args.RpcId == rpcId {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}

	fmt.Printf("put=========================================================== server[%v]: kv.lastApplyIndex: %v  args.ClientId: %v args.RpcId: %v \n", kv.me, kv.lastApplyIndex, args.ClientId, args.RpcId)
	op := Op{args.Op, args.Key, args.Value, args.ClientId, args.RpcId}
	entryIndex, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	t := 0
	for kv.lastApplyIndex < entryIndex {
		time.Sleep(50 * time.Millisecond)
		t += 1
		if t > 10 {
			reply.Err = TooLongTime
			kv.mu.Unlock()
			return
		}
		fmt.Printf("put1=========================================================== server[%v]:  kv.lastApplyIndex: %v entryIndex: %v \n", kv.me, kv.lastApplyIndex, entryIndex)
		_, isLeader1 := kv.rf.GetState()
		if !isLeader1 {
			reply.Err = ErrWrongLeaderFor
			kv.mu.Unlock()
			return
		}
	}

	// kv.clientRpc[args.ClientId] = args.RpcId

	reply.Err = OK
	kv.mu.Unlock()
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvDatabase = make(map[string]string)

	kv.clientRpc = make(map[int64]int64)


	r := bytes.NewBuffer(persister.ReadSnapshot())
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.lastApplyIndex) != nil || d.Decode(&kv.kvDatabase) != nil {
		fmt.Printf("readSnapshot error ! \n")
	}

	go kv.ServerApply()

	go kv.SnapshotToRaft(persister)

	return kv
}


func (kv *KVServer) SnapshotToRaft(persister *raft.Persister) {
	for kv.killed() == false {
		time.Sleep(500 * time.Millisecond)
		kv.mu.Lock()
		if kv.maxraftstate > 0 && persister.RaftStateSize() > kv.maxraftstate {
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.lastApplyIndex)
			e.Encode(kv.kvDatabase)
			snapshot := w.Bytes()
			kv.rf.Snapshot(kv.lastApplyIndex, snapshot)
		}
		kv.mu.Unlock()
	}
}

