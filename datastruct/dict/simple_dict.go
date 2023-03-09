package dict

// SimpleDict 简单的mapp，非线程安全，用于aof重写建立的临时辅助数据库
type SimpleDict struct {
	m map[string]interface{}
}

func MakeSimpleDict() *SimpleDict {
	return &SimpleDict{
		m: make(map[string]interface{}),
	}
}

// 检查 SimpleDict 是否没有初始化，没有初始化则返回 true
func (s *SimpleDict) notInit() bool {
	return s == nil || s.m == nil
}

func (s *SimpleDict) Get(key string) (val interface{}, exists bool) {
	val, exists = s.m[key]
	return
}

func (s *SimpleDict) Len() int {
	if s.notInit() {
		panic("m is nil")
	}
	return len(s.m)
}

func (s *SimpleDict) Put(key string, val interface{}) (result int) {
	s.m[key] = val
	return 1
}

func (s *SimpleDict) PutIfAbsent(key string, val interface{}) (result int) {
	_, exists := s.m[key]
	if exists {
		return 0
	}

	// absent, put
	s.m[key] = val
	return 1
}

func (s *SimpleDict) PutIfExists(key string, val interface{}) (result int) {
	_, exists := s.m[key]
	if !exists {
		return 0
	}

	// exists, put
	s.m[key] = val
	return 1
}

func (s *SimpleDict) Remove(key string) (result int) {
	if _, exist := s.m[key]; !exist {
		return 0
	}

	delete(s.m, key)
	return 1
}

func (s *SimpleDict) ForEach(consumer Consumer) {
	if s.notInit() {
		panic("dict is nil")
	}

	for k, v := range s.m {
		ok := consumer(k, v)
		if !ok {
			break
		}
	}
}

func (s *SimpleDict) Keys() []string {
	keys := make([]string, 0, s.Len())
	for k := range s.m {
		keys = append(keys, k)
	}

	return keys
}

func (s *SimpleDict) RandomKeys(limit int) []string {
	keys := make([]string, limit)
	for i := 0; i < limit; i++ {
		for k := range s.m {
			keys[i] = k
			break
		}
	}

	return keys
}

func (s *SimpleDict) RandomDistinctKeys(limit int) []string {
	keys := make([]string, 0, limit)
	for k := range s.m {
		if limit <= 0 {
			break
		}
		keys = append(keys, k)
		limit--
	}

	return keys
}

func (s *SimpleDict) Clear() {
	*s = *MakeSimpleDict()
}
