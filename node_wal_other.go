//go:build !linux

package asm

import "os"

// walSyncFlag makes every write to a wal segment durable before it returns. On Windows O_SYNC opens the file with
// FILE_FLAG_WRITE_THROUGH: NTFS writes the data and the metadata changes of the write (the file size) through to
// the disk, with FUA, which costs far less than FlushFileBuffers flushing the whole disk cache (~0.1 vs ~1 ms).
const walSyncFlag = os.O_SYNC
