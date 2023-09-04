package wal

import "io"

func Align(seeker io.Seeker) error {
	offset, err := seeker.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	if offset == 0 {
		return nil
	}
	quotient, remainder := offset/8, offset%8
	if remainder != 0 {
		remainder = 8
	}
	_, err = seeker.Seek(quotient*8+remainder, io.SeekStart)
	return err
}
