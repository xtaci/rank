package main

import (
	"golang.org/x/net/context"
	pb "proto"
	"sync"
)

const (
	SERVICE = "[RANK]"
)

type server struct {
	rs          RankSet
	pub_addr    string
	client_pool sync.Pool
	sync.RWMutex
}

func (s *server) init() {
	// empty tree
	s.rs.Reset()
}

func (s *server) RankChange(ctx context.Context, in *pb.Ranking_Change) (*pb.Ranking_NullResult, error) {
	s.RLock()
	defer s.RUnlock()
	s.rs.Update(in.UserId, in.Score)
	return &pb.Ranking_NullResult{}, nil
}

func (s *server) QueryRankRange(ctx context.Context, in *pb.Ranking_Range) (*pb.Ranking_RankList, error) {
	s.RLock()
	defer s.RUnlock()
	ids, cups := s.rs.GetList(int(in.StartNo), int(in.EndNo))
	return &pb.Ranking_RankList{UserIds: ids, Scores: cups}, nil
}

func (s *server) QueryUsers(ctx context.Context, in *pb.Ranking_Users) (*pb.Ranking_UserList, error) {
	s.RLock()
	defer s.RUnlock()
	ranks := make([]int32, 0, len(in.UserIds))
	scores := make([]int32, 0, len(in.UserIds))
	for _, id := range in.UserIds {
		rank, score := s.rs.Rank(id)
		ranks = append(ranks, rank)
		scores = append(scores, score)
	}
	return &pb.Ranking_UserList{Ranks: ranks, Scores: scores}, nil
}
