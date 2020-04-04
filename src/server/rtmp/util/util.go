package util

import (
	"bufio"
	"io"
	"os"
)

func Exist(filename string) bool{

	_, err := os.Stat(filename)
	return err == nil || os.IsExist(err)

}

func ReadFileLines(filename string) (lines []string, err error){

	file, err := os.OpenFile(filename, os.O_RDONLY, 0644)

	if err != nil{
		return
	}

	defer file.Close()

	bio := bufio.NewReader(file)

	for{

		var line []byte
		line, _, err = bio.ReadLine()

		if err != nil{

			if err == io.EOF{

				file.Close()
				return lines, nil

			}

			return
			
		}

		lines = append(lines, string(line))
	}

	return
}
