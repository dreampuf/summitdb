package machine

import (
	"github.com/tidwall/finn"
	"github.com/tidwall/redcon"
	"github.com/tidwall/buntdb"
	"github.com/tidwall/gjson"
	"fmt"
	"encoding/json"
)

func (m *Machine) doSmembers(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// SMEMBERS key
	if len(cmd.Args) != 2 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	return m.readDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) error {
		val, err := tx.Get(string(cmd.Args[1]))
		if err != nil {
			if err == buntdb.ErrNotFound {
				conn.WriteNull()
				return nil
			}
			return err
		}
		res := gjson.Parse(val)
		if !res.Exists() {
			conn.WriteNull()
			return nil
		}
		resMap := res.Map()
		conn.WriteArray(len(resMap))
		for key, _ := range res.Map() {
			conn.WriteBulkString(key)
		}
		return nil
	})
}

func (m *Machine) doSadd(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// SADD key member [member ...]
	if len(cmd.Args) < 3 {
		return nil, finn.ErrWrongNumberOfArguments
	}

	key := string(cmd.Args[1])
	return m.writeDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) (interface{}, error) {
		jsonValue, err := tx.Get(key)
		if err != nil && err != buntdb.ErrNotFound {
			return 0, err
		}
		result := 0
		var dict map[string]int
		err = json.Unmarshal([]byte(jsonValue), &dict)
		if err != nil {
			dict = make(map[string]int, len(cmd.Args) - 2)
		}
		// set as a string
		for i := 2; i < len(cmd.Args); i ++ {
			dict[string(cmd.Args[i])] = 1
		}
		jsonByte, err := json.Marshal(dict)
		if err != nil {
			return 0, fmt.Errorf("ERR: %v", err)
		}
		_, _, err = tx.Set(key, string(jsonByte), nil)
		if err != nil {
			return 0, err
		}
		return result, nil
	}, func(v interface{}) error {
		conn.WriteInt(v.(int))
		return nil
	})
}

func (m *Machine) doSrem(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// SREM key member [member ...]
	if len(cmd.Args) < 3 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	key := string(cmd.Args[1])
	return m.writeDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) (interface{}, error) {
		jsonValue, err := tx.Get(key)
		if err != nil {
			if err == buntdb.ErrNotFound {
				return 0, nil
			}
			return nil, err
		}
		var dict map[string]int
		err = json.Unmarshal([]byte(jsonValue), &dict)
		if err != nil {
			return nil, fmt.Errorf("ERR %v", err)
		}

		i := 2
		counter := 0
		for ; i < len(cmd.Args); i++ {
			skey := string(cmd.Args[i])
			if _, exist := dict[skey]; !exist {
				continue
			}
			delete(dict, string(cmd.Args[i]))
			counter ++
		}
		jsonByte, err := json.Marshal(dict)
		if err != nil {
			return counter, fmt.Errorf("ERR: %v", err)
		}
		_, _, err = tx.Set(key, string(jsonByte), nil)
		if err != nil {
			return counter, err
		}
		return counter, nil
	}, func(v interface{}) error {
		if v == nil {
			conn.WriteInt(0)
		} else {
			conn.WriteInt(v.(int))
		}
		return nil
	})
}
