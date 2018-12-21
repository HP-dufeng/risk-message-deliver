package core

import (
	"context"
	"io"

	pb "github.com/fengdu/risk-monitor-server/pb"

	log "github.com/sirupsen/logrus"
)

type quoteMonSubscriber struct {
	ctx    context.Context
	client pb.RiskMonitorServerClient
}

func NewQuoteMonSubscriber(ctx context.Context, client pb.RiskMonitorServerClient) Subscriber {
	return &quoteMonSubscriber{
		ctx:    ctx,
		client: client,
	}
}

func (c *quoteMonSubscriber) Subscribe(buffer int) <-chan interface{} {
	log.Infoln("SubscribeQuoteMon started...")

	out := make(chan interface{}, buffer)

	go func() {
		defer close(out)

		stream, err := c.client.SubscribeQuoteMon(c.ctx, &pb.SubscribeReq{})
		if err != nil {
			log.Errorf("SubscribeQuoteMon failed : %v", err)
			return
		}
		for {
			select {
			default:
				item, err := stream.Recv()
				if err == io.EOF {
					log.Warnf("SubscribeQuoteMon receive EOF : %v", err)
					return
				}
				if err != nil {
					log.Errorf("SubscribeQuoteMon receive failed : %v", err)
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
