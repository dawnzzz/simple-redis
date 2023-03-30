package publish

import (
	"github.com/dawnzzz/simple-redis/interface/redis"
	"sync"
)

type Publish struct {
	channels map[string]*channel // 维护各个管道，key为管道名字
	mu       sync.Mutex
}

// Subscribe 向名字为name的管道中添加订阅者
func (pub *Publish) Subscribe(client redis.Connection, names ...string) {
	pub.mu.Lock()
	defer pub.mu.Unlock()

	if pub.channels == nil {
		pub.channels = make(map[string]*channel) // 延迟初始化
	}

	for _, name := range names {
		if len(name) == 0 {
			continue
		}

		c, ok := pub.channels[name]
		if !ok {
			// 如果不存在，新建一个管道
			c = newChannel(name)
			// 开启循环发送消息
			go c.loopSendMessage()

			pub.channels[name] = c
		}

		c.addSubscriber(client)
	}

}

// Publish 在名字为name的管道中发布消息
func (pub *Publish) Publish(name string, message []byte) int {
	pub.mu.Lock()
	defer pub.mu.Unlock()

	c, ok := pub.channels[name]
	if !ok {
		// 如果不存在，直接返回
		return 0
	}

	return c.publish(message)
}

// UnSubscribe 取消订阅消息
func (pub *Publish) UnSubscribe(client redis.Connection, names ...string) {
	pub.mu.Lock()
	defer pub.mu.Unlock()

	for _, name := range names {
		c, ok := pub.channels[name]
		if !ok {
			continue
		}
		c.deleteSubscriber(client)
		if c.subscriberNum <= 0 {
			c.close()
			delete(pub.channels, name)
		}
	}

}

func (pub *Publish) ActiveChannels() []string {
	activeChannels := make([]string, 0, len(pub.channels))
	for name, channel := range pub.channels {
		if len(name) == 0 {
			continue
		}

		if channel.subscriberNum > 0 {
			activeChannels = append(activeChannels, name)
		}
	}

	return activeChannels
}

func (pub *Publish) SubscribersNum(names ...string) ([]string, []int) {
	var channels []string
	var nums []int

	if len(names) == 0 {
		// 查询所有频道的订阅者数量
		for name, channel := range pub.channels {
			if len(name) == 0 {
				continue
			}

			channels = append(channels, name)
			nums = append(nums, channel.subscriberNum)
		}

		return channels, nums
	}

	// 查询指定频道订阅者
	for _, name := range names {
		if len(name) == 0 {
			continue
		}

		num := 0
		channel, ok := pub.channels[name]
		if ok {
			num = channel.subscriberNum
		}
		channels = append(channels, name)
		nums = append(nums, num)
	}

	return channels, nums
}

func (pub *Publish) Close() {
	for _, c := range pub.channels {
		c.close()
	}
}
