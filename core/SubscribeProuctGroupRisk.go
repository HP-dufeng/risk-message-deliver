package core

import (
	"context"
	"io"

	pb "github.com/fengdu/risk-monitor-server/pb"

	log "github.com/sirupsen/logrus"
)

type prouctGroupRiskSubscriber struct {
	ctx    context.Context
	client pb.RiskMonitorServerClient
}

func NewProuctGroupRiskSubscriber(ctx context.Context, client pb.RiskMonitorServerClient) Subscriber {
	return &prouctGroupRiskSubscriber{
		ctx:    ctx,
		client: client,
	}
}

func (c *prouctGroupRiskSubscriber) Subscribe(buffer int) <-chan interface{} {
	log.Infoln("SubscribeProuctGroupRisk started...")

	out := make(chan interface{}, buffer)

	go func() {
		defer close(out)

		stream, err := c.client.SubscribeProuctGroupRisk(c.ctx, &pb.SubscribeReq{})
		if err != nil {
			log.Errorf("SubscribeProuctGroupRisk failed : %v", err)
			return
		}
		for {
			select {
			default:
				item, err := stream.Recv()
				if err == io.EOF {
					log.Warnf("SubscribeProuctGroupRisk receive EOF : %v", err)
					return
				}
				if err != nil {
					log.Errorf("SubscribeProuctGroupRisk receive failed : %v", err)
					return
				}

				msg := struct2Map(*item)
				msg["ActionKey"] = string(item.MonitorNo) + "#" + string(item.ProductGroupNo) + "#" + string(item.ContractCode)
				out <- msg
			case <-c.ctx.Done():
				return
			}

		}
	}()

	return out
}
