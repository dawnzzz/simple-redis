package pool

import (
	"errors"
	"sync"
)

type (
	FactoryFunc    func() (interface{}, error)
	FinalizerFunc  func(x interface{})
	CheckAliveFunc func(x interface{}) bool
)

var (
	ErrClosed    = errors.New("pool closed")
	ErrMaxActive = errors.New("active connections reached max num")
)

type Config struct {
	MaxIdleNum   int // 最大空闲连接数
	MaxActiveNum int // 最大活跃连接数
	MaxRetryNum  int
}

// Pool 连接池
type Pool struct {
	Config

	factory       FactoryFunc      // 创建连接
	finalizer     FinalizerFunc    // 关闭连接
	checkAlive    CheckAliveFunc   // 检查连接是否存活
	idles         chan interface{} // 空闲的连接
	activeConnNum int              // 活跃连接数
	closed        bool

	mu sync.Mutex
}

func New(factory FactoryFunc, finalizer FinalizerFunc, checkAlive CheckAliveFunc, cfg Config) *Pool {
	return &Pool{
		Config: cfg,

		factory:       factory,
		finalizer:     finalizer,
		checkAlive:    checkAlive,
		activeConnNum: 0,
		idles:         make(chan interface{}, cfg.MaxIdleNum),
		closed:        false,
	}
}

// Get 获取一个空闲连接
func (pool *Pool) Get() (interface{}, error) {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	if pool.closed {
		// 连接池已经关闭
		return nil, ErrClosed
	}

	select {
	case item := <-pool.idles:
		// 有空闲的连接
		if !pool.checkAlive(item) {
			var err error
			// 连接不存活， 创建一个连接
			item, err = pool.getItem()
			if err != nil {
				return nil, err
			}
		}

		pool.activeConnNum++ // 活跃数+1
		return item, nil
	default:
		item, err := pool.getItem()
		if err != nil {
			return nil, err
		}

		pool.activeConnNum++ // 活跃数+1
		return item, nil
	}
}

// 调用该方法时，pool.mu已经上锁
func (pool *Pool) getItem() (interface{}, error) {
	if pool.activeConnNum >= pool.MaxActiveNum { // 超过了最大活跃连接数
		return nil, ErrMaxActive
	}
	var err error
	for i := 0; i < pool.MaxRetryNum; i++ { // 最多重试三次
		item, err := pool.factory()
		if err == nil {
			return item, nil
		}
	}

	return nil, err
}

func (pool *Pool) Put(x interface{}) {
	if x == nil {
		return
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()
	if pool.closed {
		// 连接池关闭，那么就直接关闭这个连接
		pool.finalizer(x)
		pool.activeConnNum--
		return
	}

	// 将空闲的连接加入到队列中
	select {
	case pool.idles <- x:
		pool.activeConnNum--
		return
	default:
		// 已经达到最大空闲连接数量
		pool.finalizer(x)
		pool.activeConnNum--
	}
}

// Close 关闭连接池
func (pool *Pool) Close() {
	pool.mu.Lock()
	defer pool.mu.Unlock()

	pool.closed = true
	close(pool.idles)
	for item := range pool.idles { // 关闭所有空闲连接
		pool.finalizer(item)
		pool.activeConnNum--
	}
}
