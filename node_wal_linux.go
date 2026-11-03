package asm

import "syscall"

// walSyncFlag makes every write to a wal segment durable before it returns. O_DSYNC syncs the data and the
// metadata needed to read it (the file size), not the timestamps O_SYNC would sync too.
const walSyncFlag = syscall.O_DSYNC
