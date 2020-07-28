package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"path/filepath"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engines *engine_util.Engines
	config  *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	dbPath := conf.DBPath
	kvPath := filepath.Join(dbPath, "kv")
	raftPath := filepath.Join(dbPath, "raft")
	kvDB := engine_util.CreateDB("kv", conf)
	raftDB := engine_util.CreateDB("raft", conf)
	engines := engine_util.NewEngines(kvDB, raftDB, kvPath, raftPath)
	return &StandAloneStorage{
		engines: engines,
		config:  conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if err := s.engines.Close(); err != nil {
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	kvTxn := s.engines.Kv.NewTransaction(false)
	return NewStandAloneReader(kvTxn), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			put := m.Data.(storage.Put)
			txn := s.engines.Kv.NewTransaction(true)
			if err := txn.Set(engine_util.KeyWithCF(put.Cf, put.Key), put.Value); err != nil {
				return err
			}
			if err := txn.Commit(); err != nil {
				return err
			}
		case storage.Delete:
			del := m.Data.(storage.Delete)
			txn := s.engines.Kv.NewTransaction(true)
			if err := txn.Delete(engine_util.KeyWithCF(del.Cf, del.Key)); err != nil {
				return err
			}
			if err := txn.Commit(); err != nil {
				return err
			}
		}

	}
	return nil
}

type StandAloneReader struct {
	kvTxn *badger.Txn
}

func NewStandAloneReader(kvTxn *badger.Txn) *StandAloneReader {
	return &StandAloneReader{
		kvTxn: kvTxn,
	}
}

func (s *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(s.kvTxn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (s *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	iter := engine_util.NewCFIterator(cf, s.kvTxn)
	return iter
}

func (s *StandAloneReader) Close() {
	s.kvTxn.Discard()
}
