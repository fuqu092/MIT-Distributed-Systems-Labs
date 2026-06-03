package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mapValue   map[string]string
	mapVersion map[string]rpc.Tversion
	mu         sync.Mutex
}

func MakeKVServer() *KVServer {
	kv := &KVServer{mapValue: make(map[string]string), mapVersion: make(map[string]rpc.Tversion)}

	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, ok := kv.mapValue[args.Key]

	if ok == false {
		reply.Err = rpc.ErrNoKey
		return
	}

	reply.Value = kv.mapValue[args.Key]
	reply.Version = kv.mapVersion[args.Key]
	reply.Err = rpc.OK
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	_, ok := kv.mapValue[args.Key]

	if args.Version == 0 && ok == false {
		kv.mapValue[args.Key] = args.Value
		kv.mapVersion[args.Key] = 1
		reply.Err = rpc.OK

		return
	}

	if ok == false {
		reply.Err = rpc.ErrNoKey
		return
	}

	if args.Version != kv.mapVersion[args.Key] {
		reply.Err = rpc.ErrVersion
		return
	}

	kv.mapValue[args.Key] = args.Value
	kv.mapVersion[args.Key]++

	reply.Err = rpc.OK
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
