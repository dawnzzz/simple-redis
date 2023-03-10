package pool

import "testing"

func TestPool(t *testing.T) {
	factory := func() (interface{}, error) {
		return nil, nil
	}
	finalizer := func(x interface{}) {
		return
	}
	checkAlive := func(x interface{}) bool {
		return true
	}

	pool := New(factory, finalizer, checkAlive, Config{
		MaxIdleNum:   8,
		MaxActiveNum: 16,
		MaxRetryNum:  3,
	})

	items := make([]interface{}, 16)
	for i := 0; i < 16; i++ {
		item, err := pool.Get()
		if err != nil {
			t.Fatal("Get error")
		}
		items = append(items, item)
	}

	// 测试最大活跃数
	_, err := pool.Get()
	if err == nil {
		t.Fatal("MaxActive error")
	}

	// 测试最大空闲数
	for _, item := range items {
		pool.Put(item)
	}
	if len(pool.idles) != pool.MaxIdleNum {
		t.Fatal("MaxIdle error")
	}
}
