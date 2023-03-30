package timewheel

import (
	"container/list"
	"github.com/dawnzzz/simple-redis/logger"
	"time"
)

type TimeWheel struct {
	interval   time.Duration // 时间格的基本时间跨度
	ticker     *time.Ticker
	slots      []*list.List // 时间格
	currentPos int          // 当前指针，指向时间格的下标

	locations      map[string]location // 记录任务（key）所在的位置
	slotNum        int                 // 时间格的数量
	addTaskChan    chan *task          // 添加任务的通道，用于异步添加
	deleteTaskChan chan string         // 异步删除
	isClose        bool
	closeChan      chan struct{}
}

type location struct {
	slotIdx  int           // 时间格下标
	taskElem *list.Element // 指向双向链表中任务的下标
}

type task struct {
	key    string
	circle int
	delay  time.Duration
	job    func()
}

func New(interval time.Duration, slotNum int) *TimeWheel {
	if interval <= 0 || slotNum <= 0 {
		return nil
	}

	tw := &TimeWheel{
		interval:       interval,
		slots:          make([]*list.List, slotNum),
		currentPos:     0,
		locations:      make(map[string]location),
		slotNum:        slotNum,
		addTaskChan:    make(chan *task),
		deleteTaskChan: make(chan string),
		closeChan:      make(chan struct{}),
	}

	tw.initSlots()

	return tw
}

func (tw *TimeWheel) initSlots() {
	for i := 0; i < len(tw.slots); i++ {
		tw.slots[i] = list.New()
	}
}

// Start 开始时间轮
func (tw *TimeWheel) Start() {
	tw.ticker = time.NewTicker(tw.interval)
	go tw.start()
}

func (tw *TimeWheel) start() {
	for {
		select {
		case <-tw.ticker.C:
			tw.tickHandler()
		case task := <-tw.addTaskChan:
			tw.addTask(task)
		case key := <-tw.deleteTaskChan:
			tw.removeTask(key)
		case <-tw.closeChan:
			tw.ticker.Stop()
			return
		}
	}
}

// Stop 停止时间轮
func (tw *TimeWheel) Stop() {
	tw.closeChan <- struct{}{}
}

func (tw *TimeWheel) AddJob(delay time.Duration, key string, job func()) {
	if delay < 0 {
		return
	}

	tw.addTaskChan <- &task{
		key:   key,
		delay: delay,
		job:   job,
	}
}

func (tw *TimeWheel) RemoveJob(key string) {
	tw.deleteTaskChan <- key
}

func (tw *TimeWheel) tickHandler() {
	slot := tw.slots[tw.currentPos]
	if tw.currentPos == tw.slotNum-1 {
		tw.currentPos = 0
	} else {
		tw.currentPos++
	}
	go tw.scanAndRunTask(slot)
}

func (tw *TimeWheel) scanAndRunTask(slot *list.List) {
	for cur := slot.Front(); cur != nil; {
		task := cur.Value.(*task)
		if task.circle > 0 {
			task.circle--
			cur = cur.Next()
			continue
		}

		// 执行任务
		go func() {
			defer func() {
				if err := recover(); err != nil {
					logger.Error(err)
				}
			}()

			job := task.job
			job()
		}()

		// 删除任务
		next := cur.Next()
		slot.Remove(cur)
		if task.key != "" {
			delete(tw.locations, task.key)
		}
		cur = next
	}
}

func (tw *TimeWheel) addTask(t *task) {
	pos, circle := tw.getPositionAndCircle(t.delay)

	t.circle = circle

	// 插入到链表中
	slot := tw.slots[pos]
	e := slot.PushBack(t)
	loc := location{
		slotIdx:  pos,
		taskElem: e,
	}
	// 检查 key 是否之前存在，若存在则删除之前的key对应的任务
	if t.key != "" {
		_, ok := tw.locations[t.key]
		if ok {
			tw.removeTask(t.key)
		}
	}
	// 记录位置
	tw.locations[t.key] = loc
}

func (tw *TimeWheel) getPositionAndCircle(d time.Duration) (pos int, circle int) {
	delaySeconds := int(d.Seconds())
	intervalSeconds := int(tw.interval.Seconds())
	circle = int(delaySeconds / intervalSeconds / tw.slotNum)
	pos = int(tw.currentPos+delaySeconds/intervalSeconds) % tw.slotNum

	return
}

func (tw *TimeWheel) removeTask(key string) {
	// 找到位置
	loc, ok := tw.locations[key]
	if !ok {
		return
	}

	// 删除
	slot := tw.slots[loc.slotIdx]
	slot.Remove(loc.taskElem)
	delete(tw.locations, key)
}
