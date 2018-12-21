package core

import (
	"context"
	"io"

	pb "github.com/fengdu/risk-monitor-server/pb"

	log "github.com/sirupsen/logrus"
)

type tunnelRealFundSubscriber struct {
	ctx    context.Context
	client pb.RiskMonitorServerClient
}

func NewTunnelRealFundSubscriber(ctx context.Context, client pb.RiskMonitorServerClient) Subscriber {
	return &tunnelRealFundSubscriber{
		ctx:    ctx,
		client: client,
	}
}

func (c *tunnelRealFundSubscriber) Subscribe(buffer int) <-chan interface{} {
	log.Infoln("SubscribeTunnelRealFund started...")

	out := make(chan interface{}, buffer)

	go func() {
		defer close(out)

		stream, err := c.client.SubscribeTunnelRealFund(c.ctx, &pb.SubscribeReq{})
		if err != nil {
			log.Errorf("SubscribeTunnelRealFund failed : %v", err)
			return
		}
		for {
			select {
			default:
				item, err := stream.Recv()
				if err == io.EOF {
					log.Warnf("SubscribeTunnelRealFund receive EOF : %v", err)
					return
				}
				if err != nil {
					log.Errorf("SubscribeTunnelRealFund receive failed : %v", err)
					return
				}

				msg := struct2Map(*item)
				msg["ActionKey"] = string(item.MonitorNo) + "#" + string(item.TunnelCode) + "#" + string(item.CurrencyCode)
				out <- msg
			case <-c.ctx.Done():
				return
			}

		}
	}()

	return out
}
