package set

import "github.com/dawnzzz/simple-redis/datastruct/dict"

type SimpleSet struct {
	dict dict.Dict
}

func MakeSimpleSet(members ...string) Set {
	set := &SimpleSet{
		dict: dict.MakeSimpleDict(),
	}

	for _, member := range members {
		set.Add(member)
	}

	return set
}

func (set *SimpleSet) Add(val string) int {
	return set.dict.PutIfAbsent(val, nil)
}

func (set *SimpleSet) Remove(val string) int {
	return set.dict.Remove(val)
}

func (set *SimpleSet) Has(val string) bool {
	_, exists := set.dict.Get(val)

	return exists
}

func (set *SimpleSet) Len() int {
	return set.dict.Len()
}

func (set *SimpleSet) ToSlice() []string {
	slice := make([]string, 0, set.Len())

	set.ForEach(func(member string) bool {
		slice = append(slice, member)

		return true
	})

	return slice
}

func (set *SimpleSet) ForEach(consumer func(member string) bool) {
	set.dict.ForEach(func(key string, val interface{}) bool {
		return consumer(key)
	})
}

// Intersect 返回交集
func (set *SimpleSet) Intersect(another Set) Set {
	if set == nil {
		panic("set is nil")
	}

	result := MakeSimpleSet()
	another.ForEach(func(member string) bool {
		if set.Has(member) {
			result.Add(member)
		}

		return true
	})

	return result
}

// Union 合并两个集合
func (set *SimpleSet) Union(another Set) Set {
	if set == nil {
		panic("set is nil")
	}

	result := MakeSimpleSet()
	another.ForEach(func(member string) bool {
		result.Add(member)

		return true
	})

	set.ForEach(func(member string) bool {
		result.Add(member)

		return true
	})

	return result
}

// Diff 返回差集，当前集合-another
func (set *SimpleSet) Diff(another Set) Set {
	if set == nil {
		panic("set is nil")
	}

	result := MakeSimpleSet()
	set.ForEach(func(member string) bool {
		if !another.Has(member) {
			result.Add(member)
		}

		return true
	})

	return result
}

func (set *SimpleSet) RandomMembers(limit int) []string {
	return set.dict.RandomKeys(limit)
}

func (set *SimpleSet) RandomDistinctMembers(limit int) []string {
	return set.dict.RandomDistinctKeys(limit)
}
