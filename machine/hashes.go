package machine

import (
	"github.com/tidwall/finn"
	"github.com/tidwall/redcon"
	"github.com/tidwall/buntdb"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func (m *Machine) doHget(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// HGET key field
	if len(cmd.Args) != 3 {
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
		field := string(cmd.Args[2])
		res := gjson.Get(val, field)
		if !res.Exists() {
			conn.WriteNull()
			return nil
		}
		conn.WriteBulkString(res.String())
		return nil
	})
}

func (m *Machine) doHset(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// HSET key field value
	if len(cmd.Args) != 4 {
		return nil, finn.ErrWrongNumberOfArguments
	}

	key := string(cmd.Args[1])
	field := string(cmd.Args[2])
	value := string(cmd.Args[3])
	return m.writeDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) (interface{}, error) {
		json, err := tx.Get(key)
		if err != nil && err != buntdb.ErrNotFound {
			return nil, err
		}
		res := gjson.Get(json, field)
		if res.Exists() {
			return 0, nil
		}
		json, err = sjson.Set(json, field, value)
		if err != nil {
			return 0, err
		}
		_, _, err = tx.Set(key, json, nil)
		if err != nil {
			return 0, err
		}
		return 1, nil
	}, func(v interface{}) error {
		conn.WriteInt(v.(int))
		return nil
	})
}

func (m *Machine) doHmset(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// HMSET key field value [field value ...]
	if len(cmd.Args) < 4 {
		return nil, finn.ErrWrongNumberOfArguments
	}

	key := string(cmd.Args[1])
	return m.writeDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) (interface{}, error) {
		json, err := tx.Get(key)
		if err != nil && err != buntdb.ErrNotFound {
			return nil, err
		}
		for i := 2; i < len(cmd.Args); i += 2 {
			field := string(cmd.Args[i])
			value := string(cmd.Args[i + 1])
			json, err = sjson.Set(json, field, value)
			if err != nil {
				return nil, err
			}
		}
		_, _, err = tx.Set(key, json, nil)
		if err != nil {
			return nil, err
		}
		return nil, nil
	}, func(v interface{}) error {
		conn.WriteString("OK")
		return nil
	})
}
