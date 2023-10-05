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
type bDBLogger struct {
	*log.Logger
}

func (l *bDBLogger) Errorf(f string, v ...interface{}) {
	l.Printf("ERROR: "+f, v...)
}

func (l *bDBLogger) Warningf(f string, v ...interface{}) {
	l.Printf("WARNING: "+f, v...)
}

func (l *bDBLogger) Infof(f string, v ...interface{}) {
	l.Printf("INFO: "+f, v...)
}

func (l *bDBLogger) Debugf(f string, v ...interface{}) {
	l.Printf("DEBUG: "+f, v...)
}

// ====================================
//        BadgerDB simple wrapper
// ====================================

type BadgerDBWrapper struct {
	db     *badger.DB
	logger *bDBLogger
}

func MakeDB() (*BadgerDBWrapper, error) {
	var err error
	var logFile *os.File
	if logFile, err = os.OpenFile("LOG", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666); err != nil {
		fmt.Fprintf(os.Stdout, "failed to create log file\n")
		return nil, err
	}
	var defaultLogger = &bDBLogger{Logger: log.New(logFile, "[BadgerDB]", log.LstdFlags)}
	bdb := new(BadgerDBWrapper)
	bdb.logger = defaultLogger
	return bdb, nil
}

func (d *BadgerDBWrapper) Open(opt badger.Options) error {
	var err error
	opt.Logger = d.logger
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
	return nil
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
