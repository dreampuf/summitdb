package machine

import (
	"github.com/tidwall/finn"
	"github.com/tidwall/redcon"
	"github.com/tidwall/buntdb"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"strconv"
	"encoding/json"
	"fmt"
)


func (m *Machine) doLlen(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// LLEN key

	if len(cmd.Args) < 2 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	key := string(cmd.Args[1])
	return m.readDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) error {
		val, err := tx.Get(key)
		if err != nil {
			if err == buntdb.ErrNotFound {
				conn.WriteInt(0)
				return nil
			}
			return err
		}
		res := int(gjson.Get(val, "#").Int())
		conn.WriteInt(res)
		return nil
	})
}

func (m *Machine) doLrange(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// LRANGE key start stop
	//#TBD

	if len(cmd.Args) < 4 {
		return nil, finn.ErrWrongNumberOfArguments
	}
	key := string(cmd.Args[1])
	start, err := strconv.Atoi(string(cmd.Args[2]))
	if err != nil {
		return nil, finn.ErrUnknownCommand
	}
	stop, err := strconv.Atoi(string(cmd.Args[3]))
	if err != nil {
		return nil, finn.ErrUnknownCommand
	}
	return m.readDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) error {
		json, err := tx.Get(key)
		if err != nil {
			if err == buntdb.ErrNotFound {
				conn.WriteArray(0)
				return nil
			}
			return err
		}
		ary := gjson.Parse(json).Array()
		if ary == nil || len(ary) == 0 {
			conn.WriteArray(0)
			return nil
		}
		if start < 0 {
			start = len(ary) + start
		}
		if stop < 0 {
			stop = len(ary) + stop
		}
		if start > stop {
			conn.WriteArray(0)
			return nil
		}
		conn.WriteArray(stop - start)
		for n := start; n <= stop; n ++ {
			conn.WriteBulkString(ary[n].String())
		}
		return nil
	})
}

func (m *Machine) doLpush(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// LPUSH key value [value ...]
	if len(cmd.Args) < 3 {
		return nil, finn.ErrWrongNumberOfArguments
	}

	key := string(cmd.Args[1])
	return m.writeDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) (interface{}, error) {
		json, err := tx.Get(key)
		var result int = 0
		if err != nil && err != buntdb.ErrNotFound {
			return result, err
		}
		if json == "" {
			json = "[]"
		}
		for i := 2; i < len(cmd.Args); i ++ {
			json, err = sjson.Set(json, "-1", string(cmd.Args[i]))
			if err != nil {
				return result, err
			}
		}
		result = int(gjson.Get(json, "#").Int())
		_, _, err = tx.Set(key, json, nil)
		if err != nil {
			return result, err
		}
		return result, nil
	}, func(v interface{}) error {
		conn.WriteInt(v.(int))
		return nil
	})
}

func (m *Machine) doLpop(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// LPOP key
	if len(cmd.Args) != 2 {
		return nil, finn.ErrWrongNumberOfArguments
	}

	key := string(cmd.Args[1])
	return m.writeDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) (interface{}, error) {
		json, err := tx.Get(key)
		if err != nil && err != buntdb.ErrNotFound {
			return nil, err
		}
		res := gjson.Get(json, "0")
		if !res.Exists() {
			return nil, err
		}
		json, err = sjson.Delete(json, "0")
		if err != nil {
			return nil, err
		}
		_, _, err = tx.Set(key, json, nil)
		if err != nil {
			return nil, err
		}
		return res.Str, nil
	}, func(v interface{}) error {
		if v == nil {
			conn.WriteNull()
		} else {
			str, _ := v.(string)
			conn.WriteBulkString(str)
		}
		return nil
	})
}


func (m *Machine) doRpoplpush(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// RPOPLPUSH source destination
	if len(cmd.Args) != 3 {
		return nil, finn.ErrWrongNumberOfArguments
	}

	source := string(cmd.Args[1])
	destination := string(cmd.Args[2])
	return m.writeDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) (interface{}, error) {
		// RPOP
		sourceJson, err := tx.Get(source)
		if err != nil && err != buntdb.ErrNotFound {
			return nil, err
		}
		sourceRes := gjson.Parse(sourceJson).Array()
		if sourceRes == nil || len(sourceRes) == 0 {
			return nil, err
		}
		sourceItem := sourceRes[len(sourceRes)-1]
		sourceJson, err = sjson.Delete(sourceJson, "-1")
		if err != nil {
			return nil, err
		}
		_, _, err = tx.Set(source, sourceJson, nil)
		if err != nil {
			return nil, err
		}

		// LPUSH
		destJson, err := tx.Get(destination)
		if err != nil && err != buntdb.ErrNotFound {
			return nil, err
		}
		if err == buntdb.ErrNotFound {
			destJson = "[]"
		}
		destJson, err = sjson.Set(destJson, "0", sourceItem.Str)
		if err != nil {
			return nil, err
		}
		_, _, err = tx.Set(destination, destJson, nil)
		if err != nil {
			return nil, err
		}
		return sourceItem.Str, nil
	}, func(v interface{}) error {
		if v == nil {
			conn.WriteNull()
		} else {
			str, _ := v.(string)
			conn.WriteBulkString(str)
		}
		return nil
	})
}

func (m *Machine) doLrem(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// LREM key count value
	//    count > 0: Remove elements equal to value moving from head to tail.
	//    count < 0: Remove elements equal to value moving from tail to head.
	//    count = 0: Remove all elements equal to value.
	if len(cmd.Args) != 4 {
		return nil, finn.ErrWrongNumberOfArguments
	}

	key := string(cmd.Args[1])
	count, err := strconv.Atoi(string(cmd.Args[2]))
	if err != nil {
		return nil, finn.ErrUnknownCommand
	}
	value := string(cmd.Args[3])
	return m.writeDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) (interface{}, error) {
		jsonValue, err := tx.Get(key)
		if err != nil && err != buntdb.ErrNotFound {
			return 0, err
		}
		var ary []string
		err = json.Unmarshal([]byte(jsonValue), &ary)
		if err != nil {
			return 0, fmt.Errorf("ERR: %v", err)
		}
		if len(ary) == 0 {
			return 0, nil
		}
		// equal 0 solution by default
		effected := 0
		n := len(ary)
		start := 0
		delta := 1
		if count > 0 {
			n = count
		} else if count < 0 {
			start = len(ary) - 1
			delta = -1
			n = - count
		}
		newAry := []string{}
		for i := start; ; i += delta {
			if ary[i] != value {
				newAry = append(newAry)
				effected ++
			}
			n--
			if n == 0 { break }
		}

		jsonByte, err := json.Marshal(newAry)
		if err != nil {
			return effected, fmt.Errorf("ERR: %v", err)
		}
		_, _, err = tx.Set(key, string(jsonByte), nil)
		if err != nil {
			return effected, err
		}
		return effected, nil
	}, func(v interface{}) error {
		if v == nil {
			conn.WriteNull()
		} else {
			val, _ := v.(int)
			conn.WriteInt(val)
		}
		return nil
	})
}
