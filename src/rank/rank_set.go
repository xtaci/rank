package main

import (
	log "github.com/GameGophers/libs/nsq-logger"
	"gopkg.in/vmihailenco/msgpack.v2"
	"sync"
)

import (
	"rank/dos"
	"rank/ss"
)

const (
	UPPER_THRESHOLD = 4096 // storage changed to tree when elements exceeds this
	LOWER_THRESHOLD = 2048 // storage changed to sortedset when elements below this
)

// a ranking set
type RankSet struct {
	R dos.Tree        // 排名
	S ss.SortedSet    // sorted-set
	M map[int32]int32 // 映射 ID  => SCORE
	sync.RWMutex
}

func (r *RankSet) init() {
	r.M = make(map[int32]int32)
}

// convert to tree
func (r *RankSet) to_tree() {
	for k, v := range r.M {
		r.R.Insert(v, k)
	}
	log.Tracef("convert sortedset to rbtree %v", len(r.M))
}

func (r *RankSet) Update(id, newscore int32) {
	// 集合排名锁
	r.Lock()
	defer r.Unlock()

	oldscore, ok := r.M[id]
	if !ok { // new
		if len(r.M) > UPPER_THRESHOLD {
			r.to_tree()
			r.R.Insert(newscore, id)
		} else {
			r.S.Insert(id, newscore)
		}
		r.M[id] = newscore
		return
	} else {
		if len(r.M) > UPPER_THRESHOLD {
			_, n := r.R.Locate(oldscore, id)
			r.R.Delete(id, n)
			r.R.Insert(newscore, id)
		} else {
			r.S.Update(id, newscore)
		}
		r.M[id] = newscore
		return
	}
}

func (r *RankSet) Count() int32 {
	r.RLock()
	defer r.RUnlock()
	return int32(len(r.M))
}

// range [A,B]
func (r *RankSet) GetList(A, B int) (ids []int32, scores []int32) {
	if A < 1 || A > B {
		return
	}

	// 集合排名锁
	r.RLock()
	defer r.RUnlock()

	if A > len(r.M) {
		return
	}

	if B > len(r.M) {
		B = len(r.M)
	}

	if len(r.M) > UPPER_THRESHOLD {
		ids, scores = make([]int32, B-A+1), make([]int32, B-A+1)
		for i := A; i <= B; i++ {
			id, n := r.R.Rank(i)
			ids[i-A] = id
			scores[i-A] = n.Score()
		}
	} else {
		return r.S.GetList(A, B)
	}

	return
}

// rank of a user
func (r *RankSet) Rank(userid int32) (rank int32, score int32) {
	// 集合排名锁
	r.RLock()
	defer r.RUnlock()

	if len(r.M) > UPPER_THRESHOLD {
		rankno, _ := r.R.Locate(r.M[userid], userid)
		return int32(rankno), r.M[userid]
	} else {
		rankno := r.S.Locate(userid)
		return int32(rankno), r.M[userid]
	}
}

// serialization
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
	if len(r.M) > UPPER_THRESHOLD {
		for id, score := range m {
			r.R.Insert(score, id)
		}
		log.Tracef("rank restored into rbtree %v", len(r.M))
	} else {
		for id, score := range m {
			r.S.Insert(id, score)
		}
		log.Tracef("rank restored into sortedset %v", len(r.M))
	}

	return nil
}
