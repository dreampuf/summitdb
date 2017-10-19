package machine

import (
	"github.com/tidwall/finn"
	"github.com/tidwall/redcon"
	"github.com/tidwall/buntdb"
	"github.com/tidwall/gjson"
	"fmt"
	"strconv"
	"sort"
	"encoding/json"
)

/*

Data Structure: { key1: score1, key2: score2 }

 */

func (m *Machine) doZcard(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// ZCARD key

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
		conn.WriteInt(len(resMap))
		return nil
	})
}

func (m *Machine) doZadd(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// ZADD key [NX|XX] [CH] [INCR] score member [score member ...]
	// Only support 'ZADD key score member [score member ...]'
	if len(cmd.Args) < 4 {
		return 0, finn.ErrWrongNumberOfArguments
	}

	key := string(cmd.Args[1])
	result := 0
	return m.writeDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) (interface{}, error) {
		jsonval, err := tx.Get(key)
		if err != nil && err != buntdb.ErrNotFound {
			return result, err
		}
		var dict map[string]float64
		err = json.Unmarshal([]byte(jsonval), &dict)
		if err != nil {
			dict = make(map[string]float64)
		}

		// set as a string
		for i := 2; i < len(cmd.Args); i += 2 {
			kkey := string(cmd.Args[i+1])
			kscore, err := strconv.ParseFloat(string(cmd.Args[i]), 64)
			if err != nil {
				return result, fmt.Errorf("ERR %v", err)
			}
			_, exist := dict[kkey]
			if !exist {
				result += 1
			}
			dict[kkey] = kscore
		}
		jsonByte, err := json.Marshal(dict)
		if err != nil {
			return result, err
		}
		_, _, err = tx.Set(key, string(jsonByte), nil)
		if err != nil {
			return result, err
		}
		return result, nil
	}, func(v interface{}) error {
		n, _ := v.(int)
		conn.WriteInt(n)
		return nil
	})
}

func (m *Machine) doZrem(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	//ZREM key member [member ...]

	if len(cmd.Args) < 3 {
		return nil, finn.ErrWrongNumberOfArguments
	}

	key := string(cmd.Args[1])
	result := 0
	return m.writeDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) (interface{}, error) {
		jsonval, err := tx.Get(key)
		if err != nil && err != buntdb.ErrNotFound {
			return result, err
		}

		var dict map[string]float64
		err = json.Unmarshal([]byte(jsonval), &dict)
		if err != nil {
			dict = make(map[string]float64)
		}

		// set as a string
		for i := 2; i < len(cmd.Args); i ++ {
			member := string(cmd.Args[i])
			if _, exist := dict[member]; !exist {
				continue
			}
			delete(dict, member)
			result ++
		}
		jsonByte, err := json.Marshal(dict)
		if err != nil {
			return result, fmt.Errorf("ERR %v", err)
		}
		_, _, err = tx.Set(key, string(jsonByte), nil)
		if err != nil {
			return result, err
		}
		return result, nil
	}, func(v interface{}) error {
		conn.WriteInt(result)
		return nil
	})
}

func (m *Machine) doZrangebyscore(a finn.Applier, conn redcon.Conn, cmd redcon.Command, tx *buntdb.Tx) (interface{}, error) {
	// ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
	alen := len(cmd.Args)
	if alen < 4 {
		return nil, finn.ErrWrongNumberOfArguments
	}

	var min, max float64
	var counter, offset, count int
	key := string(cmd.Args[1])
	counter = 0
	offset = 0
	count = -1
	min, err := strconv.ParseFloat(string(cmd.Args[2]), 64)
	if err != nil {
		return nil, fmt.Errorf("ERR parse min failed %v", err)
	}
	max, err = strconv.ParseFloat(string(cmd.Args[3]), 64)
	if err != nil {
		return nil, fmt.Errorf("ERR parse max failed %v", err)
	}
	WITHSCORES := false
	if (alen == 5 && string(cmd.Args[5]) == "WITHSCORES") || (alen == 8 && string(cmd.Args[5]) == "WITHSCORES") {
		WITHSCORES = true
	}

	if string(cmd.Args[alen-3]) == "LIMIT" {
		offset, err = strconv.Atoi(string(cmd.Args[alen-2]))
		if err != nil {
			return nil, fmt.Errorf("ERR parse offset failed %v", err)
		}
		count, err = strconv.Atoi(string(cmd.Args[alen-1]))
		if err != nil {
			return nil, fmt.Errorf("ERR parse count failed %v", err)
		}
	}

	return m.readDoApply(a, conn, cmd, tx, func(tx *buntdb.Tx) error {
		json, err := tx.Get(key)
		if err != nil && err != buntdb.ErrNotFound {
			return err
		}

		type kv struct {
			Key   string
			Value float64
		}
		var klist []kv
		for k, v := range gjson.Parse(json).Map() {
			klist = append(klist, kv{k, v.Float()})
		}
		sort.Slice(klist, func(i, j int) bool {
			return klist[i].Value > klist[j].Value
		})

		type strpair struct { k, score string }
		var resultList []strpair
		total := 0
		for _, i := range klist {
			if i.Value < min || i.Value > max {
				continue
			}
			if counter < offset {
				continue
			}
			counter += 1
			resultList = append(resultList, strpair{ i.Key, strconv.FormatFloat(i.Value, 'E', -1, 64)})
			if counter < count {
				break
			}
			total ++
		}
		if WITHSCORES {
			conn.WriteArray(len(resultList) * 2)
		} else {
			conn.WriteArray(len(resultList))
		}
		for _, item := range resultList {
			conn.WriteBulkString(item.k)
			if WITHSCORES {
				conn.WriteBulkString(item.score)
			}
		}
		return nil
	})
}
