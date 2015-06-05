package main

import (
	"errors"
	"golang.org/x/net/context"
	pb "proto"
	"sync"
)

const (
	SERVICE = "[RANK]"
)

var (
	ERROR_NAME_NOT_EXISTS = errors.New("name not exists")
)

type server struct {
	ranks map[string]*RankSet
	sync.Mutex
}

func (s *server) init() {
	s.ranks = make(map[string]*RankSet)
}

func (s *server) lock_do(f func()) {
	s.Lock()
	defer s.Unlock()
	f()
}

func (s *server) RankChange(ctx context.Context, in *pb.Ranking_Change) (*pb.Ranking_NullResult, error) {
	// check name existence
	var rs *RankSet
	s.lock_do(func() {
		rs = s.ranks[in.Name]
		if rs == nil {
			rs = &RankSet{}
			rs.init()
			s.ranks[in.Name] = rs
		}
	})

	// apply update one the rankset
	rs.Update(in.UserId, in.Score)
	return &pb.Ranking_NullResult{}, nil
}

func (s *server) QueryRankRange(ctx context.Context, in *pb.Ranking_Range) (*pb.Ranking_RankList, error) {
	var rs *RankSet
	s.lock_do(func() {
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
	s.lock_do(func() {
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
