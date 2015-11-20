package goberkeleydb

import (
	"bytes"
	"fmt"
	"testing"
)

func TestHoge(t *testing.T) {
	db, err := OpenBDB(NoEnv, NoTxn, "hoge.db", "", BTree, DB_CREATE, 0)
	if err != nil {
		t.Fatalf("open failed %v\n", err)
	}
	defer db.Close(0)

	fmt.Printf("open\n")
	err = db.Put(NoTxn, []byte("hoge"), []byte("fuga"), 0)
	if err != nil {
		t.Fatalf("put failed %v\n", err)
	}
	fmt.Printf("put\n")
	val, err := db.Get(NoTxn, []byte("hoge"), 0)
	if err != nil {
		t.Fatalf("get failed %v\n", err)
	}
	fmt.Printf("get\n")
	if !bytes.Equal(val, []byte("fuga")) {
		t.Fatalf("expected %v actual %v\n", "fuga", val)
	}
	fmt.Printf("done\n")
}

func TestCursor(t *testing.T) {
	db, err := OpenBDB(NoEnv, NoTxn, "hoge.db", "", BTree, DB_RDONLY, 0)
	if err != nil {
		t.Fatalf("open failed %v\n", err)
	}
	defer db.Close(0)
	fmt.Printf("open\n")

	cur, err := db.NewCursor(NoTxn, 0)
	if err != nil {
		t.Fatalf("open cursor failed %v\n", err)
	}
	defer cur.Close()
	key, val, err := cur.First()
	if err != nil {
		t.Fatalf("cur get failed %v\n", err)
	}

	fmt.Printf("%v : %v\n", key, val)
}
