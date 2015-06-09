package main

import (
	log "github.com/GameGophers/nsq-logger"
	"gopkg.in/vmihailenco/msgpack.v2"
	"sync"
)

import (
	"rank/dos"
)

// 排名集合
type RankSet struct {
	R dos.Tree        // 排名
	M map[int32]int32 // 映射 ID  => SCORE
	sync.RWMutex
}

// 初始化排名集
func (r *RankSet) init() {
	r.M = make(map[int32]int32)
}

// 排名更新
func (r *RankSet) Update(id, newscore int32) {
	// 集合排名锁
	r.Lock()
	defer r.Unlock()

	oldscore, ok := r.M[id]
	if !ok { // 新玩家
		r.R.Insert(newscore, id)
		r.M[id] = newscore
		return
	} else { // 老玩家
		_, n := r.R.Locate(oldscore, id)
		if n == nil {
			log.Critical("没有在DOS中查到玩家:", id)
			return
		}

		r.R.Delete(id, n)
		r.R.Insert(newscore, id)
		r.M[id] = newscore
		return
	}
}

// 获得总个数
func (r *RankSet) Count() int32 {
	// 集合排名锁
	r.RLock()
	defer r.RUnlock()
	return int32(r.R.Count())
}

// 范围读取 [A,B]
func (r *RankSet) GetList(A, B int) (ids []int32, scores []int32) {
	if A < 1 || A > B {
		return
	}

	// 集合排名锁
	r.RLock()
	defer r.RUnlock()

	if A > r.R.Count() {
		return
	}

	if B > r.R.Count() {
		B = r.R.Count()
	}

	ids, scores = make([]int32, B-A+1), make([]int32, B-A+1)
	for i := A; i <= B; i++ {
		id, n := r.R.Rank(i)
		ids[i-A] = id
		scores[i-A] = n.Score()
	}

	return
}

// 读取某个玩家的排名
func (r *RankSet) Rank(userid int32) (rank int32, score int32) {
	// 集合排名锁
	r.RLock()
	defer r.RUnlock()

	rankno, _ := r.R.Locate(r.M[userid], userid)
	return int32(rankno), r.M[userid]
}

// 序列化
func (r *RankSet) Marshal() ([]byte, error) {
	r.RLock()
	defer r.RUnlock()
	return msgpack.Marshal(r.M)
}

func (r *RankSet) Unmarshal(bin []byte) error {
	m := make(map[int32]int32)
	r.Lock()
	defer r.Unlock()
	err := msgpack.Unmarshal(bin, &m)
	if err != nil {
		return err
	}

	// 还原
	r.M = m
	for id, score := range m {
		r.R.Insert(score, id)
	}

	return nil
}
