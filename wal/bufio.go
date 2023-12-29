package wal

import (
	"os"
)

type BufFile struct {
	*os.File
}

func (b *BufFile) Discard(n int) (int, error) {
	if n == 0 {
		return 0, nil
	}

	v := make([]byte, n)
	return b.Read(v)
}
