package goberkeleydb

import (
	_ "os"
	_ "reflect"
	"unsafe"
)

/*
 #cgo LDFLAGS: -ldb
 #include <stdlib.h>
 #include <db.h>
 static inline int db_open(DB *db, DB_TXN *txn, const char *file, const char *database, DBTYPE type, u_int32_t flags, int mode) {
 	return db->open(db, txn, file, database, type, flags, mode);
 }
 static inline int db_close(DB *db, u_int32_t flags) {
 	return db->close(db, flags);
 }
 static inline int db_get_type(DB *db, DBTYPE *type) {
 	return db->get_type(db, type);
 }
 static inline int db_put(DB *db, DB_TXN *txn, DBT *key, DBT *data, u_int32_t flags) {
 	return db->put(db, txn, key, data, flags);
 }
 static inline int db_get(DB *db, DB_TXN *txn, DBT *key, DBT *data, u_int32_t flags) {
 	return db->get(db, txn, key, data, flags);
 }
 static inline int db_del(DB *db, DB_TXN *txn, DBT *key, u_int32_t flags) {
 	return db->del(db, txn, key, flags);
 }
 static inline int db_cursor(DB *db, DB_TXN *txn, DBC **cursor, u_int32_t flags) {
 	return db->cursor(db, txn, cursor, flags);
 }
 static inline int db_cursor_close(DBC *cur) {
 	return cur->close(cur);
 }
 static inline int db_cursor_get(DBC *cur, DBT *key, DBT *data, u_int32_t flags) {
 	return cur->get(cur, key, data, flags);
 }
 static inline int db_cursor_del(DBC *cur, u_int32_t flags) {
 	return cur->del(cur, flags);
 }

 static inline int db_env_open(DB_ENV *env, const char *home, u_int32_t flags, int mode) {
 	return env->open(env, home, flags, mode);
 }
 static inline int db_env_close(DB_ENV *env, u_int32_t flags) {
 	return env->close(env, flags);
 }

 static inline int db_env_txn_begin(DB_ENV *env, DB_TXN *parent, DB_TXN **txn, u_int32_t flags) {
 	return env->txn_begin(env, parent, txn, flags);
 }
 static inline int db_txn_abort(DB_TXN *txn) {
 	return txn->abort(txn);
 }
 static inline int db_txn_commit(DB_TXN *txn, u_int32_t flags) {
 	return txn->commit(txn, flags);
 }
*/
import "C"

type BerkeleyDB struct {
	ptr *C.DB
}
type Transaction struct {
	ptr *C.DB_TXN
}
type Cursor struct {
	db  BerkeleyDB
	ptr *C.DBC
}
type DbType int

type DBT struct {
	ptr *C.DBT
}

// Available database types.
const (
	BTree    = DbType(C.DB_BTREE)
	Hash     = DbType(C.DB_HASH)
	Numbered = DbType(C.DB_RECNO)
	Queue    = DbType(C.DB_QUEUE)
	Unknown  = DbType(C.DB_UNKNOWN)
)

type Environment struct {
	ptr *C.DB_ENV
}

func Err(C.int) error {
	return nil
}

func BytesDbt(val []byte) *DBT {
	return &DBT{
		ptr: &C.DBT{
			data: unsafe.Pointer(&val[0]),
			size: C.u_int32_t(len(val)),
		},
	}
}

func DbtBytes(val *DBT) []byte {
	return C.GoBytes(val.ptr.data, C.int(val.ptr.size))
}

func OpenBDB(env Environment, trn Transaction, file string, database string, dbtype DbType, flags uint32, mode int) (*BerkeleyDB, error) {
	cFile := C.CString(file)
	defer C.free(unsafe.Pointer(cFile))
	cDatabase := C.CString(database)
	defer C.free(unsafe.Pointer(cDatabase))

	var db *BerkeleyDB = new(BerkeleyDB)

	err := Err(C.db_create(&db.ptr, env.ptr, 0))
	if err != nil {
		return nil, err
	}

	err = Err(C.db_open(db.ptr, trn.ptr, cFile, cDatabase, C.DBTYPE(dbtype), C.u_int32_t(flags), C.int(mode)))
	if err != nil {
		db.Close(0)
		return nil, err
	}
	return db, nil
}
func (db BerkeleyDB) Close(flags uint32) (err error) {
	if db.ptr != nil {
		err = Err(C.db_close(db.ptr, C.u_int32_t(flags)))
		db.ptr = nil
	}
	return err
}
func (db BerkeleyDB) GetType() (DbType, error) {
	var cdbtype C.DBTYPE
	err := Err(C.db_get_type(db.ptr, &cdbtype))
	dbtype := DbType(cdbtype)
	return dbtype, err
}
func (db BerkeleyDB) Put(txn Transaction, key, val DBT, flags uint32) error {
	cFlags := C.u_int32_t(flags)
	return Err(C.db_put(db.ptr, txn.ptr, key.ptr, val.ptr, cFlags))
}
func (db BerkeleyDB) Get(txn Transaction, key DBT, flag uint32) (DBT, error) {
	cFlags := C.u_int32_t(flag)
    var cVal DBT
	err := Err(C.db_get(db.ptr, txn.ptr, key.ptr, cVal.ptr, cFlags))
	return cVal, err
}
func (db BerkeleyDB) Del(txn Transaction, key []byte, flag uint32) error {
	return nil
}

func (db BerkeleyDB) NewCursor(txn Transaction, flag uint32) (*Cursor, error) {
	return nil, nil
}
func (cursor *Cursor) Close() error {
	return nil
}
func (cursor *Cursor) Get(key []byte, val []byte, flag uint32) error {
	return nil
}
func (cursor *Cursor) Del(flag uint32) error {
	return nil
}

func NewEnvironment(home string, flag uint32, mode int) (*Environment, error) {
	return nil, nil
}
func (env Environment) Close(flag uint32) error {
	return nil
}

func (env Environment) BeginTransaction(flag uint32) (*Transaction, error) {
	return nil, nil
}
func (trx Transaction) Abort() error {
	return nil
}
func (txn Transaction) Commit(flag uint32) error {
	return nil
}
