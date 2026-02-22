package main

import (
	"bytes"
	"encoding/json"
)

type KV struct {
	log Log
	mem map[string][]byte
}

func (kv *KV) Open() error {
	if err := kv.log.Open(); err != nil {
		return err
	}

	kv.mem = map[string][]byte{}
	for {
		ent := Entry{}
		eof, err := kv.log.Read(&ent)
		if err != nil {
			return err
		}

		if eof {
			break
		}

		if ent.deleted {
			delete(kv.mem, string(ent.key))
		} else {
			kv.mem[string(ent.key)] = ent.val
		}
	}
	return nil
}

func (kv *KV) Close() error { return kv.log.Close() }

// 取得
func (kv *KV) Get(key []byte) ([]byte, bool, error) {
	val, ok := kv.mem[string(key)]
	return val, ok, nil
}

// 保存
func (kv *KV) Set(key []byte, val []byte) (bool, error) {
	prev, exist := kv.mem[string(key)]
	kv.mem[string(key)] = val
	updated := !exist || !bytes.Equal(prev, val)
	if updated {
		if err := kv.log.Write(&Entry{key: key, val: val}); err != nil {
			return false, err
		}
	}
	return updated, nil
}

// 削除
func (kv *KV) Del(key []byte) (bool, error) {
	_, deleted := kv.mem[string(key)]
	if deleted {
		if err := kv.log.Write(&Entry{key: key, deleted: true}); err != nil {
			return false, err
		}
		delete(kv.mem, string(key))
	}
	return deleted, nil
}

// 一覧
func (kv *KV) List() ([]byte, error) {
	if kv.mem == nil {
		return []byte("{}"), nil
	}

	exportData := make(map[string]string)
	for key, val := range kv.mem {
		exportData[key] = string(val)
	}

	jsonData, err := json.MarshalIndent(exportData, "", "  ")
	if err != nil {
		return nil, err
	}

	return jsonData, nil
}
