package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/fengdu/websocket-message-deliver/core"
	"google.golang.org/grpc"

	pb "github.com/fengdu/risk-monitor-server/pb"
	log "github.com/sirupsen/logrus"
	r "gopkg.in/rethinkdb/rethinkdb-go.v5"
)

var (
	rethinkAddr = flag.String("RethinkDB__Url", "websocket-rethinkdb:28015", "The rethinkdb server address in the format of host:port")
	serverAddr  = flag.String("RiskMonitorServer__Url", "riskmonitorserver:31002", "The server address in the format of host:port")
	buffer      = flag.Int("Buffer", 1000, "Buffer size for subscription operations")
)

func main() {
	log.SetFormatter(&log.TextFormatter{
		ForceColors:     true,
		FullTimestamp:   true,
		TimestampFormat: time.Stamp,
	})

	flag.Parse()
	parseEnv()

	go func() {
		for {
			func() {
				session := checkIfRethinkDBReady(*rethinkAddr)
				defer session.Close()
				checkIfRiskMonitorServerReady(*serverAddr)
				subscribes(*serverAddr, session, *buffer)
			}()
		}

	}()

	// Wait for Control C to exit
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)

	// Block until a signal is received
	<-ch
	log.Println("Stopping the server")
}

func parseEnv() {
	rethinkAddrEnv := os.Getenv("RethinkDB__Url")
	if rethinkAddrEnv != "" {
		rethinkAddr = &rethinkAddrEnv
	}
	serverAddrEnv := os.Getenv("RiskMonitorServer__Url")
	if serverAddrEnv != "" {
		serverAddr = &serverAddrEnv
	}
	bufferEnv := os.Getenv("Buffer")
	if bufferEnv != "" {
		if i, err := strconv.Atoi(bufferEnv); err != nil {
			buffer = &i
		}
	}

	log.Println("RethinkDB__Url: ", *rethinkAddr)
	log.Println("RiskMonitorServer__Url: ", *serverAddr)
	log.Println("Buffer: ", *buffer)
}

func checkIfRethinkDBReady(rethinkAddr string) *r.Session {
	delimiter()
	defer delimiter()
	log.Infoln("Check if rethinkdb ready ...")
	defer log.Infoln("Check if rethinkdb ready ... ok")

	waitForDBReady := make(chan *r.Session)

	go func() {
		for {
			session, err := r.Connect(r.ConnectOpts{
				Address:    rethinkAddr, // endpoint without http
				MaxOpen:    100,
				InitialCap: 8,
			})
			if err != nil {
				log.Errorln(err)
				time.Sleep(5 * time.Second)
				continue
			}
			log.Infof("RethinkDB connected successed : %v")

			err = core.CreateDBAndTableAfterDrop(session)
			if err != nil {
				log.Errorln(err)
				session.Close()
				time.Sleep(5 * time.Second)
				continue
			}
			log.Infoln("Database and tables init completed")

			waitForDBReady <- session
			return
		}
	}()

	session := <-waitForDBReady

	return session
}

func checkIfRiskMonitorServerReady(serverAddr string) {
	delimiter()
	defer delimiter()
	log.Infoln("Check if RiskMonitorServer ready ...")
	defer log.Infoln("Check if RiskMonitorServer ready ... ok")

	waitForHealthCheckSuccessed := make(chan struct{})
	go func() {
		for {
			err := func() error {
				conn, err := grpc.Dial(serverAddr, grpc.WithInsecure())
				if err != nil {
					log.Errorf("RiskMonitorServer grpc dial failed : %v", err)
					return err
				}
				defer conn.Close()
				client := pb.NewRiskMonitorServerClient(conn)

				if _, err := client.HeartBeatReq(context.Background(), &pb.HeartBeat{}); err != nil {
					log.Errorf("RiskMonitorServer health check failed : %v", err)
					return err
				}

				return nil
			}()

			if err == nil {
				break
			}

			time.Sleep(5 * time.Second)
		}

		waitForHealthCheckSuccessed <- struct{}{}
	}()

	<-waitForHealthCheckSuccessed
	log.Infoln("RiskMonitorServer health check successed")
}

func subscribes(serverAddr string, session *r.Session, buffer int) {
	delimiter()
	defer delimiter()
	log.Infoln("Subscribe started")

	clientDeadline := time.Now().Add(7 * time.Duration(24) * time.Hour)
	ctx, cancel := context.WithDeadline(context.Background(), clientDeadline)
	defer cancel()

	conn, err := grpc.Dial(serverAddr, grpc.WithInsecure())
	if err != nil {
		log.Errorf("RiskMonitorServer grpc dial failed : %v", err)
		return
	}
	defer conn.Close()
	client := pb.NewRiskMonitorServerClient(conn)

	done := make(chan struct{})

	do := func(subscriber core.Subscriber, pipeline core.Pipeline) {
		defer log.Warnf("%T defered", subscriber)
		c := subscriber.Subscribe(buffer)
		msg := pipeline.BufferCount(c)
		pipeline.Write(msg)
		select {
		case done <- struct{}{}:
			return
		default:
			return
		}
	}

	go do(core.NewCustRiskSubscriber(ctx, client), core.NewPipeline(ctx, session, core.TableName_SubscribeCustRisk))
	go do(core.NewQuoteMonSubscriber(ctx, client), core.NewPipeline(ctx, session, core.TableName_SubscribeQuoteMon))
	go do(core.NewTunnelRealFundSubscriber(ctx, client), core.NewPipeline(ctx, session, core.TableName_SubscribeTunnelRealFund))
	go do(core.NewCorpHoldMonSubscriber(ctx, client), core.NewPipeline(ctx, session, core.TableName_SubscribeCorpHoldMon))
	go do(core.NewCustHoldSubscriber(ctx, client), core.NewPipeline(ctx, session, core.TableName_SubscribeCustHold))
	go do(core.NewCsustGroupHoldSubscriber(ctx, client), core.NewPipeline(ctx, session, core.TableName_SubscribeCustGroupHold))
	go do(core.NewNearDediveHoldSubscriber(ctx, client), core.NewPipeline(ctx, session, core.TableName_SubscribeNearDediveHold))
	go do(core.NewProuctGroupRiskSubscriber(ctx, client), core.NewPipeline(ctx, session, core.TableName_SubscribeProuctGroupRisk))

	<-done
}

func delimiter() {
	log.Println("------------------------------------------------------------------------------------")
}
