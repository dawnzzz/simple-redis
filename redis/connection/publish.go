package connection

func (c *Connection) AddSubscribeChannel(names ...string) {
	if c.subscribeChannels == nil {
		c.subscribeChannels = map[string]struct{}{}
	}

	for _, name := range names {
		c.subscribeChannels[name] = struct{}{}
	}
}

func (c *Connection) CancelSubscribeChannel(names ...string) {
	for _, name := range names {
		delete(c.subscribeChannels, name)
	}
}

func (c *Connection) GetSubscribeNum() int {
	return len(c.subscribeChannels)
}

func (c *Connection) GetSubscribes() []string {
	if len(c.subscribeChannels) == 0 {
		return []string{}
	}

	subscribes := make([]string, 0, len(c.subscribeChannels))
	for subName := range c.subscribeChannels {
		subscribes = append(subscribes, subName)
	}

	return subscribes
}
