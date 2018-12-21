package core

import (
	"context"
	"io"

	pb "github.com/fengdu/risk-monitor-server/pb"

	log "github.com/sirupsen/logrus"
)

type custRiskSubscriber struct {
	ctx    context.Context
	client pb.RiskMonitorServerClient
}

func NewCustRiskSubscriber(ctx context.Context, client pb.RiskMonitorServerClient) Subscriber {
	return &custRiskSubscriber{
		ctx:    ctx,
		client: client,
	}
}

func (c *custRiskSubscriber) Subscribe(buffer int) <-chan interface{} {
	log.Infoln("SubscribeCustRisk started...")

	out := make(chan interface{}, buffer)

	go func() {
		defer close(out)

		stream, err := c.client.SubscribeCustRisk(c.ctx, &pb.SubscribeReq{})
		if err != nil {
			log.Errorf("SubscribeCustRisk failed : %v", err)
			return
		}
		for {
			select {
			default:
				item, err := stream.Recv()
				if err == io.EOF {
					log.Warnf("SubscribeCustRisk receive EOF : %v", err)
					return
				}
				if err != nil {
					log.Errorf("SubscribeCustRisk receive failed : %v", err)
					return
				}

				msg := struct2Map(*item)
				msg["ActionKey"] = string(item.MonitorNo) + "#" + string(item.CustNo) + "#" + string(item.CurrencyCode)

				out <- msg
			case <-c.ctx.Done():
				return
			}

		}
	}()

	return out
}
