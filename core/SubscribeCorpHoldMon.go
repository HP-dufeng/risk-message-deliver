package core

import (
	"context"
	"io"

	pb "github.com/fengdu/risk-monitor-server/pb"

	log "github.com/sirupsen/logrus"
)

type corpHoldMonSubscriber struct {
	ctx    context.Context
	client pb.RiskMonitorServerClient
}

func NewCorpHoldMonSubscriber(ctx context.Context, client pb.RiskMonitorServerClient) Subscriber {
	return &corpHoldMonSubscriber{
		ctx:    ctx,
		client: client,
	}
}

func (c *corpHoldMonSubscriber) Subscribe(buffer int) <-chan interface{} {
	log.Infoln("SubscribeCorpHoldMon started...")

	out := make(chan interface{}, buffer)

	go func() {
		defer close(out)

		stream, err := c.client.SubscribeCorpHoldMon(c.ctx, &pb.SubscribeReq{})
		if err != nil {
			log.Errorf("SubscribeCorpHoldMon failed : %v", err)
			return
		}
		for {
			select {
			default:
				item, err := stream.Recv()
				if err == io.EOF {
					log.Warnf("SubscribeCorpHoldMon receive EOF : %v", err)
					return
				}
				if err != nil {
					log.Errorf("SubscribeCorpHoldMon receive failed : %v", err)
					return
				}

				msg := struct2Map(*item)
				msg["ActionKey"] = string(item.MonitorNo) + "#" + string(item.ContractCode)
				out <- msg
			case <-c.ctx.Done():
				return
			}

		}
	}()

	return out
}
