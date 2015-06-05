package main

import (
	"bytes"
	"errors"
	"fmt"
	log "github.com/GameGophers/nsq-logger"
	nsq "github.com/bitly/go-nsq"
	"github.com/coreos/go-etcd/etcd"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"gopkg.in/vmihailenco/msgpack.v2"
	"math/rand"
	"net/http"
	"os"
	pb "proto"
	services "services/proto"
	"strings"
	"sync"
)

const (
	SERVICE            = "[RANK]"
	TOPIC              = "SCORE_CHANGE"
	DEFAULT_ETCD       = "127.0.0.1:2379"
	ENV_NSQLOOKUPD     = "NSQLOOKUPD_HOST"
	ENV_NSQD           = "NSQD_HOST"
	DEFAULT_PUB_ADDR   = "http://127.0.0.1:4151/pub?topic=" + TOPIC
	DEFAULT_NSQLOOKUPD = "127.0.0.1:4160"
	NSQ_IN_FLIGHT      = 128
)

const (
	SNOWFLAKE_SERVICE_NAME = "/backends/snowflake"
)

// score change message format
type score_change_msg struct {
	UserId int32 `msgpack:"a"`
	Score  int32 `msgpack:"b"`
}

type server struct {
	rs          RankSet
	pub_addr    string
	client_pool sync.Pool
	sync.RWMutex
}

func (s *server) init() {
	// empty tree
	s.rs.Reset()

	// determine pub address
	s.pub_addr = DEFAULT_PUB_ADDR
	if env := os.Getenv(ENV_NSQD); env != "" {
		s.pub_addr = env + "/pub?topic=" + TOPIC
	}

	log.Trace("will publish to:", s.pub_addr)

	// etcd client
	machines := []string{DEFAULT_ETCD}
	if env := os.Getenv("ETCD_HOST"); env != "" {
		machines = strings.Split(env, ";")
	}
	s.client_pool.New = func() interface{} {
		return etcd.NewClient(machines)
	}

	// rank change subscriber
	s.init_subscriber(s.create_ephermal_channel())
}

func (s *server) create_ephermal_channel() string {
	client := s.client_pool.Get().(*etcd.Client)
	defer func() {
		s.client_pool.Put(client)
	}()
	resp, err := client.Get(SNOWFLAKE_SERVICE_NAME, false, true)
	if err != nil {
		log.Critical(err)
		os.Exit(-1)
	}

	// random choose a service
	if len(resp.Node.Nodes) == 0 {
		log.Critical("snowflake service not started yet?")
		os.Exit(-1)
	}

	// dial grpc
	conn, err := grpc.Dial(resp.Node.Nodes[rand.Intn(len(resp.Node.Nodes))].Value)
	if err != nil {
		log.Critical("did not connect: %v", err)
		os.Exit(-1)
	}

	// save client
	c := services.NewSnowflakeServiceClient(conn)
	// create consumer from lookupds
	r, err := c.GetUUID(context.Background(), &services.Snowflake_NullRequest{})
	if err != nil {
		log.Critical(err)
		os.Exit(-1)
	}

	return fmt.Sprintf("RANK%v#ephemeral", r.Uuid)
}

func (s *server) RankChange(ctx context.Context, in *pb.Ranking_Change) (*pb.Ranking_NullResult, error) {
	// send to nsqd
	msg, err := msgpack.Marshal(score_change_msg{UserId: in.UserId, Score: in.Score})
	if err != nil {
		log.Critical(err)
		return nil, errors.New("internal error, cannot marshal change")
	}

	// publish
	_, err = http.Post(s.pub_addr, "", bytes.NewReader(msg))
	if err != nil {
		log.Critical(err)
		return nil, errors.New("internal error, cannot post to nsqd")
	}
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

func (s *server) init_subscriber(channel string) {
	log.Trace("unique ephermal channel name:", channel)
	cfg := nsq.NewConfig()
	cfg.MaxInFlight = NSQ_IN_FLIGHT
	consumer, err := nsq.NewConsumer(TOPIC, channel, cfg)
	if err != nil {
		log.Critical(err)
		os.Exit(-1)
	}

	// message process
	consumer.AddHandler(nsq.HandlerFunc(func(msg *nsq.Message) error {
		// process update
		chg := &score_change_msg{}
		err := msgpack.Unmarshal(msg.Body, chg)
		if err != nil {
			log.Error("unmarshal failed")
			return nil
		}
		s.Lock()
		defer s.Unlock()
		s.rs.Update(chg.UserId, chg.Score)
		log.Trace("userid:", chg.UserId, " score:", chg.Score)
		return nil
	}))

	// read environtment variable
	addresses := []string{DEFAULT_NSQLOOKUPD}
	if env := os.Getenv(ENV_NSQLOOKUPD); env != "" {
		addresses = strings.Split(env, ";")
	}

	// connect to nsqlookupd
	log.Trace("connect to nsqlookupds ip:", addresses)
	if err := consumer.ConnectToNSQLookupds(addresses); err != nil {
		log.Critical(err)
		return
	}
	log.Info("nsqlookupd connected")
}
