package main

import (
	"badgerBench/bDB"
	"fmt"
	"log"

	"github.com/dgraph-io/badger"
	//"os"
	// "time"
)

var dbDir string = "/tmp/BadgerBench"


func main() {
	fmt.Println("open db")

	opt := badger.DefaultOptions(dbDir)

	db, err := bDB.MakeDB(opt)
	db.Open()
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	defer db.DestroyDB()
	key := "key0"
	if err = db.Put(key, "hello, world!"); err != nil {
		log.Fatal(err)
	}
	var val string
	if val, err = db.Get(key); err != nil {
		log.Fatal(err)
	}
	fmt.Println("key:", key, ", value:", val)
}
