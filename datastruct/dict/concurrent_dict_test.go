package dict

import (
	"strconv"
	"testing"
)

func TestConcurrentDict(t *testing.T) {
	dict := MakeConcurrentDict(31)

	// put and get test
	dict.Put("k1", "v1")
	if v, exist := dict.Get("k1"); !exist || v != "v1" {
		t.Error("Put k1 error")
	}
	dict.PutIfExists("k1", "new v1")
	if v, exist := dict.Get("k1"); !exist || v != "new v1" {
		t.Error("PutIfExists k1 error")
	}
	dict.PutIfExists("k2", "new v1")
	if _, exist := dict.Get("k2"); exist {
		t.Error("PutIfExists error")
	}
	dict.PutIfAbsent("k2", "v2")
	if v, exist := dict.Get("k2"); !exist || v != "v2" {
		t.Error("PutIfAbsent error")
	}
	dict.PutIfAbsent("k2", "new v2")
	if v, exist := dict.Get("k2"); !exist || v != "v2" {
		t.Error("PutIfAbsent error")
	}

	// Keys
	for i := 1; i <= 10; i++ {
		iStr := strconv.Itoa(i)
		dict.Put("k"+iStr, "v"+iStr)
	}
	t.Log(dict.Keys())

	// RandomKeys
	t.Log("RandomKeys:", dict.RandomKeys(5))
	t.Log("RandomKeys:", dict.RandomKeys(5))

	// RandomDistinctKeys
	t.Log("RandomDistinctKeys:", dict.RandomDistinctKeys(5))
	t.Log("RandomDistinctKeys:", dict.RandomDistinctKeys(5))

	dict.ForEach(func(key string, val interface{}) bool {
		t.Log("key=", key, " val=", val)
		return true
	})
}
