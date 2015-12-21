package bdb

import (
	"unsafe"
)

/*
 #cgo LDFLAGS: /usr/lib/x86_64-linux-gnu/libdb.a
 #include <stdlib.h>
 #include <errno.h>
 #include <db.h>
 static inline int db_open(DB *db, DB_TXN *txn, const char *file, const char *database, DBTYPE type, u_int32_t flags, int mode) {
 	return db->open(db, txn, file, database, type, flags, mode);
 }
 static inline int db_close(DB *db, u_int32_t flags) {
 	return db->close(db, flags);
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

 static inline int db_env_open(DB_ENV *env, const char *home, u_int32_t flags, int mode) {
 	return env->open(env, home, flags, mode);
 }
 static inline int db_env_close(DB_ENV *env, u_int32_t flags) {
 	return env->close(env, flags);
 }

*/
import "C"

type BerkeleyDB struct {
	ptr *C.DB
}
type Transaction struct {
	ptr *C.DB_TXN
}
type Environment struct {
	ptr *C.DB_ENV
}
type DBT struct {
	ptr *C.DBT
}
type Cursor struct {
	ptr *C.DBC
}

var NoEnv = Environment{ptr: nil}
var NoTxn = Transaction{ptr: nil}

type DbType int

const (
	BTree    = DbType(C.DB_BTREE)
	Hash     = DbType(C.DB_HASH)
	Numbered = DbType(C.DB_RECNO)
	Queue    = DbType(C.DB_QUEUE)
	Unknown  = DbType(C.DB_UNKNOWN)
)

type DbFlag uint32

const (
	DbAutoCommit     = DbFlag(C.DB_AUTO_COMMIT)
	DbCreate         = DbFlag(C.DB_CREATE)
	DbExcl           = DbFlag(C.DB_EXCL)
	DbMultiVersion   = DbFlag(C.DB_MULTIVERSION)
	DbNoMmap         = DbFlag(C.DB_NOMMAP)
	DbReadOnly       = DbFlag(C.DB_RDONLY)
	DbReadUncommited = DbFlag(C.DB_READ_UNCOMMITTED)
	DbThread         = DbFlag(C.DB_THREAD)
	DbTruncate       = DbFlag(C.DB_TRUNCATE)
)

type Errno int

const (
	ErrAgain           = Errno(C.EAGAIN)
	ErrInvalid         = Errno(C.EINVAL)
	ErrNoEntry         = Errno(C.ENOENT)
	ErrExists          = Errno(C.EEXIST)
	ErrAccess          = Errno(C.EACCES)
	ErrNoSpace         = Errno(C.ENOSPC)
	ErrPermission      = Errno(C.EPERM)
	ErrRunRecovery     = Errno(C.DB_RUNRECOVERY)
	ErrVersionMismatch = Errno(C.DB_VERSION_MISMATCH)
	ErrOldVersion      = Errno(C.DB_OLD_VERSION)
	ErrLockDeadlock    = Errno(C.DB_LOCK_DEADLOCK)
	ErrLockNotGranted  = Errno(C.DB_LOCK_NOTGRANTED)
	ErrBufferTooSmall  = Errno(C.DB_BUFFER_SMALL)
	ErrSecondaryBad    = Errno(C.DB_SECONDARY_BAD)
	ErrForeignConflict = Errno(C.DB_FOREIGN_CONFLICT)
	ErrKeyExists       = Errno(C.DB_KEYEXIST)
	ErrKeyEmpty        = Errno(C.DB_KEYEMPTY)
	ErrNotFound        = Errno(C.DB_NOTFOUND)
)

var ok error

func (err Errno) Error() string {
	return C.GoString(C.db_strerror(C.int(err)))
}

func Err(rc C.int) error {
	if rc != 0 {
		return Errno(rc)
	} else {
		return ok
	}
}

func bytesDBT(val []byte) *C.DBT {
	return &C.DBT{
		data:  unsafe.Pointer(&val[0]),
		size:  C.u_int32_t(len(val)),
		flags: C.DB_DBT_USERMEM,
	}
}

func cloneToBytes(val *C.DBT) []byte {
	return C.GoBytes(val.data, C.int(val.size))
}

func OpenBDB(env Environment, txn Transaction, file string, database *string, dbtype DbType, flags DbFlag, mode int) (*BerkeleyDB, error) {
	cFile := C.CString(file)
	defer C.free(unsafe.Pointer(cFile))

	var cDatabase *C.char
	if database != nil {
		cDatabase = C.CString(*database)
		defer C.free(unsafe.Pointer(cDatabase))
	}

	var db *BerkeleyDB = new(BerkeleyDB)

	//The flags parameter is currently unused, and must be set to 0.
	// https://docs.oracle.com/cd/E17276_01/html/api_reference/C/dbcreate.html
	err := Err(C.db_create(&db.ptr, env.ptr, 0))
	if err != nil {
		return nil, err
	}

	err = Err(C.db_open(db.ptr, txn.ptr, cFile, cDatabase, C.DBTYPE(dbtype), C.u_int32_t(flags), C.int(mode)))
	if err != nil {
		db.Close(0)
		return nil, err
	}
	return db, ok
}
func (db BerkeleyDB) Close(flags DbFlag) error {
	if db.ptr == nil {
		return ok
	}

	err := Err(C.db_close(db.ptr, C.u_int32_t(flags)))
	if err != nil {
		return err
	}

	db.ptr = nil
	return ok
}
func (db BerkeleyDB) Put(txn Transaction, key, val []byte, flags DbFlag) error {
	return Err(C.db_put(db.ptr, txn.ptr, bytesDBT(key), bytesDBT(val), C.u_int32_t(flags)))
}
func (db BerkeleyDB) Get(txn Transaction, key []byte, flags DbFlag) ([]byte, error) {
	data := C.DBT{flags: C.DB_DBT_REALLOC}
	defer C.free(data.data)

	err := Err(C.db_get(db.ptr, txn.ptr, bytesDBT(key), &data, C.u_int32_t(flags)))
	if err != nil {
		return nil, err
	}

	return cloneToBytes(&data), ok
}
func (db BerkeleyDB) Del(txn Transaction, key []byte, flags DbFlag) error {
	return Err(C.db_del(db.ptr, txn.ptr, bytesDBT(key), C.u_int32_t(flags)))
}

func (db BerkeleyDB) NewCursor(txn Transaction, flags DbFlag) (*Cursor, error) {
	ret := new(Cursor)
	err := Err(C.db_cursor(db.ptr, txn.ptr, &ret.ptr, C.u_int32_t(flags)))
	if err != nil {
		return nil, err
	}
	return ret, ok
}
func (cursor Cursor) Close() error {
	if cursor.ptr == nil {
		return ok
	}
	err := Err(C.db_cursor_close(cursor.ptr))
	if err != nil {
		return err
	}
	cursor.ptr = nil
	return ok
}
func (cursor Cursor) First() ([]byte, []byte, error) {
	return cursor.CursorGetRaw(C.DB_FIRST)
}
func (cursor Cursor) Next() ([]byte, []byte, error) {
	return cursor.CursorGetRaw(C.DB_NEXT)
}
func (cursor Cursor) Last() ([]byte, []byte, error) {
	return cursor.CursorGetRaw(C.DB_LAST)
}
func (cursor Cursor) CursorGetRaw(flags DbFlag) ([]byte, []byte, error) {
	key := C.DBT{flags: C.DB_DBT_REALLOC}
	defer C.free(key.data)
	val := C.DBT{flags: C.DB_DBT_REALLOC}
	defer C.free(val.data)

	err := Err(C.db_cursor_get(cursor.ptr, &key, &val, C.u_int32_t(flags)))
	if err != nil {
		return nil, nil, err
	}
	return cloneToBytes(&key), cloneToBytes(&val), ok
}
func NewEnvironment(home string, flags DbFlag, mode int) (*Environment, error) {
	ret := new(Environment)

	err := Err(C.db_env_create(&ret.ptr, 0))
	if err != nil {
		return nil, err
	}

	cHome := C.CString(home)
	defer C.free(unsafe.Pointer(cHome))

	err = Err(C.db_env_open(ret.ptr, cHome, C.u_int32_t(flags), C.int(mode)))
	if err != nil {
		ret.Close(0)
		return nil, err
	}

	return ret, ok
}
func (env Environment) Close(flags DbFlag) error {
	if env.ptr == nil {
		return ok
	}

	err := Err(C.db_env_close(env.ptr, C.u_int32_t(flags)))
	if err != nil {
		return err
	}

	env.ptr = nil
	return ok
}
