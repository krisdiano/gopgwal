package wal

import "io"

func Align(seeker io.Seeker) (int64, error) {
	offset, err := seeker.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	if offset == 0 {
		return 0, nil
	}
	quotient, remainder := offset/8, offset%8
	if remainder != 0 {
		remainder = 8
	}
	return seeker.Seek(quotient*8+remainder, io.SeekStart)
}
