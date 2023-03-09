package utils

import (
	"os"
)

func GetFileSizeByName(filename string) int64 {
	file, err := os.Open(filename)
	defer func() {
		_ = file.Close()
	}()
	if err != nil {
		return 0
	}
	fileStat, err := file.Stat()
	if err != nil {
		return 0
	}
	fileSize := fileStat.Size()

	return fileSize
}
