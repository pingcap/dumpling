package dumpling

import (
	"fmt"
)

type Writer interface {
	WriteDatabases([]string) error
	WriteTable(string) error
	WriteData()
}

type dummyWriter struct{}

func (dummyWriter) WriteDatabases(dbs []string) error {
	fmt.Println("write databases", dbs)
	return nil
}

func (dummyWriter) WriteTable(table string) error {
	fmt.Println("write table", table)
	return nil
}

func (dummyWriter) WriteData() {
	fmt.Println("write data")
}
