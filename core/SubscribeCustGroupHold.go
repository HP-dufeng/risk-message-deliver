package core

import (
	"context"
	"io"
	"strconv"

	pb "github.com/fengdu/risk-monitor-server/pb"

	log "github.com/sirupsen/logrus"
)

type sustGroupHoldSubscriber struct {
	ctx    context.Context
	client pb.RiskMonitorServerClient
}

func NewCsustGroupHoldSubscriber(ctx context.Context, client pb.RiskMonitorServerClient) Subscriber {
	return &sustGroupHoldSubscriber{
		ctx:    ctx,
		client: client,
	}
}

func (c *sustGroupHoldSubscriber) Subscribe(buffer int) <-chan interface{} {
	log.Infoln("SubscribeCustGroupHold started...")

	out := make(chan interface{}, buffer)

	go func() {
		defer close(out)

		stream, err := c.client.SubscribeCustGroupHold(c.ctx, &pb.SubscribeReq{})
		if err != nil {
			log.Errorf("SubscribeCustGroupHold failed : %v", err)
			return
		}
		for {
			select {
			default:
				item, err := stream.Recv()
				if err == io.EOF {
					log.Warnf("SubscribeCustGroupHold receive EOF : %v", err)
					return
				}
				if err != nil {
					log.Errorf("SubscribeCustGroupHold receive failed : %v", err)
					return
				}

				msg := struct2Map(*item)
				msg["ActionKey"] = string(item.MonitorNo) + "#" + string(item.CustGroupNo) + "#" + string(item.ContractCode) + "#" + strconv.Itoa(int(item.HoldType))
				out <- msg
			case <-c.ctx.Done():
				return
			}

		}
	}()

	return out
}
