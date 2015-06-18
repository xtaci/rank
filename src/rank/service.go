package main

import (
	"errors"
	log "github.com/GameGophers/libs/nsq-logger"
	"github.com/boltdb/bolt"
	"golang.org/x/net/context"
	"os"
	"os/signal"
	pb "proto"
	"sync"
	"syscall"
	"time"
)

const (
	SERVICE = "[RANK]"
)

const (
	BOLTDB_FILE    = "/data/RANK-DUMP.DAT"
	BOLTDB_BUCKET  = "RANKING"
	CHANGES_SIZE   = 65536
	CHECK_INTERVAL = 10 * time.Second // if ranking has changed, how long to check
)

var (
	ERROR_NAME_NOT_EXISTS = errors.New("name not exists")
)

type server struct {
	ranks   map[string]*RankSet
	pending chan string
	sync.RWMutex
}

func (s *server) init() {
	s.ranks = make(map[string]*RankSet)
	s.pending = make(chan string, CHANGES_SIZE)
	s.restore()
	go s.persistence_task()
}

func (s *server) lock_read(f func()) {
	s.RLock()
	defer s.RUnlock()
	f()
}

func (s *server) lock_write(f func()) {
	s.Lock()
	defer s.Unlock()
	f()
}

func (s *server) RankChange(ctx context.Context, in *pb.Ranking_Change) (*pb.Ranking_NullResult, error) {
	// check name existence
	var rs *RankSet
	s.lock_write(func() {
		rs = s.ranks[in.Name]
		if rs == nil {
			rs = &RankSet{}
			rs.init()
			s.ranks[in.Name] = rs
		}
	})

	// apply update one the rankset
	rs.Update(in.UserId, in.Score)
	s.pending <- in.Name
	return &pb.Ranking_NullResult{}, nil
}

func (s *server) QueryRankRange(ctx context.Context, in *pb.Ranking_Range) (*pb.Ranking_RankList, error) {
	var rs *RankSet
	s.lock_read(func() {
		rs = s.ranks[in.Name]
	})

	if rs == nil {
		return nil, ERROR_NAME_NOT_EXISTS
	}

	ids, cups := rs.GetList(int(in.StartNo), int(in.EndNo))
	return &pb.Ranking_RankList{UserIds: ids, Scores: cups}, nil
}

func (s *server) QueryUsers(ctx context.Context, in *pb.Ranking_Users) (*pb.Ranking_UserList, error) {
	var rs *RankSet
	s.lock_read(func() {
		rs = s.ranks[in.Name]
	})

	if rs == nil {
		return nil, ERROR_NAME_NOT_EXISTS
	}

	ranks := make([]int32, 0, len(in.UserIds))
	scores := make([]int32, 0, len(in.UserIds))
	for _, id := range in.UserIds {
		rank, score := rs.Rank(id)
		ranks = append(ranks, rank)
		scores = append(scores, score)
	}
	return &pb.Ranking_UserList{Ranks: ranks, Scores: scores}, nil
}

// persistence ranking tree into db
func (s *server) persistence_task() {
	timer := time.After(CHECK_INTERVAL)
	db := s.open_db()
	changes := make(map[string]bool)
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM)

	for {
		select {
		case key := <-s.pending:
			changes[key] = true
		case <-timer:
			s.dump_changes(db, changes)
			log.Infof("perisisted %v trees:", len(changes))
			changes = make(map[string]bool)
			timer = time.After(CHECK_INTERVAL)
		case <-sig:
			s.dump_changes(db, changes)
			db.Close()
			os.Exit(0)
		}
	}
}

func (s *server) dump_changes(db *bolt.DB, changes map[string]bool) {
	for k := range changes {
		// marshal
		var rs *RankSet
		s.lock_read(func() {
			rs = s.ranks[k]
		})
		if rs == nil {
			log.Error("empty rankset:", k)
			continue
		}

		// serialization and save
		bin, err := rs.Marshal()
		if err != nil {
			log.Critical("cannot marshal:", err)
			os.Exit(-1)
		}

		db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(BOLTDB_BUCKET))
			err := b.Put([]byte(k), bin)
			return err
		})
	}
}

func (s *server) open_db() *bolt.DB {
	db, err := bolt.Open(BOLTDB_FILE, 0600, nil)
	if err != nil {
		log.Critical(err)
		os.Exit(-1)
	}
	// create bulket
	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(BOLTDB_BUCKET))
		if err != nil {
			log.Criticalf("create bucket: %s", err)
			os.Exit(-1)
		}
		return nil
	})
	return db
}

func (s *server) restore() {
	// restore data from db file
	db := s.open_db()
	defer db.Close()
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(BOLTDB_BUCKET))
		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			rs := &RankSet{}
			rs.init()
			err := rs.Unmarshal(v)
			if err != nil {
				log.Critical("rank data corrupted:", err)
				os.Exit(-1)
			}
			s.ranks[string(k)] = rs
		}

		return nil
	})
}
