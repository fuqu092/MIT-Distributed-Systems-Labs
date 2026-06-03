package lock

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck          kvtest.IKVClerk
	currVersion rpc.Tversion
	lockname    string
	clientId    string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck, lockname: lockname, clientId: kvtest.RandValue(8)}
	return lk
}

func (lk *Lock) Acquire() {
	for {
		val, version, err := lk.ck.Get(lk.lockname)
		if err == rpc.ErrNoKey {
			lk.currVersion = 0
		} else if val == "free" {
			lk.currVersion = version
		} else if val == lk.clientId {
			break
		} else {
			continue
		}

		err = lk.ck.Put(lk.lockname, lk.clientId, lk.currVersion)

		if err == rpc.OK {
			break
		}
	}
}

func (lk *Lock) Release() {
	// Your code here
	lk.ck.Put(lk.lockname, "free", lk.currVersion+1)
}
