package bDB

import (
	"fmt"
	"github.com/dgraph-io/badger"
	"log"
	"os"
)

// ====================================
//
//	BadgerDB Logger
//
// ====================================

// log level
const  (
	DEBUG int = iota
	INFO 
	WARNING
	ERROR
)

type bDBLogger struct {
	*log.Logger
	level int
}

func (l *bDBLogger) Errorf(f string, v ...interface{}) {
	if l.level <= ERROR {
		l.Printf("ERROR: "+f, v...)
	}
}

func (l *bDBLogger) Warningf(f string, v ...interface{}) {
	if l.level <= WARNING {
		l.Printf("WARNING: "+f, v...)
	}
}

func (l *bDBLogger) Infof(f string, v ...interface{}) {
	if l.level <= INFO {
		l.Printf("INFO: "+f, v...)
	}
}

func (l *bDBLogger) Debugf(f string, v ...interface{}) {
	if l.level <= DEBUG {
		l.Printf("DEBUG: "+f, v...)
	}
}


// ====================================
//        BadgerDB simple wrapper
// ====================================

type BadgerDBWrapper struct {
	db     *badger.DB
}

func MakeDB() (*BadgerDBWrapper) {
	return new(BadgerDBWrapper)
}

func (d *BadgerDBWrapper) Open(opt badger.Options) error {
	var err error
	var logFile *os.File
	if _,err := os.Stat(opt.Dir); os.IsNotExist(err) {
		// if db dir not exist, then create
		if err = os.Mkdir(opt.Dir, 0777); err != nil {
			fmt.Fprintf(os.Stderr, "failed to create db dir\n")
			return err
		}
	} else if err != nil {
		return err
	}
	if logFile, err = os.OpenFile(opt.Dir + "/LOG", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666); err != nil {
		fmt.Fprintf(os.Stderr, "failed to create log file\n")
		return err
	}
	var defaultLogger = &bDBLogger{Logger: log.New(logFile, "[BadgerDB]", log.LstdFlags), level: WARNING}
	opt.Logger = defaultLogger
	if d.db, err = badger.Open(opt); err != nil {
		return err
	}
	return nil
}

func (d *BadgerDBWrapper) Put(key, value string, f ...func(*badger.Txn) error) error {
	wb := d.db.NewWriteBatch()
	defer wb.Cancel()
	if err := wb.SetEntry(badger.NewEntry([]byte(key), []byte(value)).WithMeta(0)); err != nil {
		return err
	}
	if err := wb.Flush(); err != nil {
		return err
	}
	// return d.db.Update(func(txn *badger.Txn) error{
	// 	k := []byte(key)
	// 	v := []byte(value)
	// 	return txn.Set(k, v)
	// })
	return nil
}

func (d *BadgerDBWrapper) NewWriteBatch() *badger.WriteBatch{
	return d.db.NewWriteBatch()
}

func (d *BadgerDBWrapper) DoView(f func(*badger.Txn) error) error {
	return d.db.View(f)
}

func (d *BadgerDBWrapper) Get(key string) (string, error) {
	var value string
	err := d.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if err != nil {
			return err
		}
		err = item.Value(func(val []byte) error {
			value = string(val)
			return nil
		})
		return err
	})
	return value, err
}

func (d *BadgerDBWrapper) Close() error {
	return d.db.Close()
}

func (d *BadgerDBWrapper) DestroyDB() error {
	return d.db.DropAll()
}
