package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

type GetWorkArgs struct{}

type WorkDoneArgs struct {
	WorkId int // 1 for maptask, 2 for reduce task
	TaskId int // contains maptaskid or reducetaskid of completed task
}

type GetWorkReply struct {
	WorkId   int    // 0 for no work, 1 for map task, 2 for reduce task
	TaskId   int    // contains maptaskid or reducetaskid
	Filename string // contains filename in case of map task
}

type WorkDoneReply struct{}
