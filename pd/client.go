// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package pd

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"google.golang.org/grpc"
)

// Client is a PD (Placement Driver) client.
// It should not be used after calling Close().
type Client interface {
	GetClusterID(ctx context.Context) uint64
	AllocID(ctx context.Context) (uint64, error)
	Bootstrap(ctx context.Context, store *metapb.Store, region *metapb.Region) (*pdpb.BootstrapResponse, error)
	IsBootstrapped(ctx context.Context) (bool, error)
	PutStore(ctx context.Context, store *metapb.Store) error
	GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error)
	GetAllStores(ctx context.Context, excludeTombstone bool) ([]*metapb.Store, error)
	GetClusterConfig(ctx context.Context) (*metapb.Cluster, error)
	GetRegion(ctx context.Context, key []byte) (*metapb.Region, error)
	GetRegionByID(ctx context.Context, regionID uint64) (*metapb.Region, error)
	ReportRegion(*pdpb.RegionHeartbeatRequest)
	AskSplit(ctx context.Context, region *metapb.Region) (*pdpb.AskSplitResponse, error)
	AskBatchSplit(ctx context.Context, region *metapb.Region, count int) (*pdpb.AskBatchSplitResponse, error)
	ReportBatchSplit(ctx context.Context, regions []*metapb.Region) error
	GetGCSafePoint(ctx context.Context) (uint64, error)
	StoreHeartbeat(ctx context.Context, stats *pdpb.StoreStats) error
	SetRegionHeartbeatResponseHandler(h func(*pdpb.RegionHeartbeatResponse))
	Close()
}

const (
	pdTimeout             = time.Second
	maxInitClusterRetries = 100
)

var (
	// errFailInitClusterID is returned when failed to load clusterID from all supplied PD addresses.
	errFailInitClusterID = errors.New("[pd] failed to get cluster id")
)

type client struct {
	url        string
	tag        string
	clusterID  uint64
	clientConn *grpc.ClientConn

	receiveRegionHeartbeatCh chan *pdpb.RegionHeartbeatResponse
	regionCh                 chan *pdpb.RegionHeartbeatRequest

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	heartbeatHandler atomic.Value
}

// NewClient creates a PD client.
func NewClient(pdAddr string, tag string) (Client, error) {
	log.Infof("[%s][pd] create pd client with endpoints %v", tag, pdAddr)
	ctx, cancel := context.WithCancel(context.Background())
	c := &client{
		url:                      pdAddr,
		receiveRegionHeartbeatCh: make(chan *pdpb.RegionHeartbeatResponse, 1),
		ctx:                      ctx,
		cancel:                   cancel,
		tag:                      tag,
		regionCh:                 make(chan *pdpb.RegionHeartbeatRequest, 64),
	}
	cc, err := c.createConn()
	if err != nil {
		return nil, errors.Trace(err)
	}
	c.clientConn = cc
	if err := c.initClusterID(); err != nil {
		return nil, errors.Trace(err)
	}
	log.Infof("[%s][pd] init cluster id %v", tag, c.clusterID)
	c.wg.Add(1)
	go c.heartbeatStreamLoop()

	return c, nil
}

func (c *client) pdClient() pdpb.PDClient {
	return pdpb.NewPDClient(c.clientConn)
}

func (c *client) initClusterID() error {
	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()
	for i := 0; i < maxInitClusterRetries; i++ {
		members, err := c.getMembers(ctx)
		if err != nil || members.GetHeader() == nil {
			log.Errorf("[%s][pd] failed to get cluster id: %v", c.tag, err)
			continue
		}
		c.clusterID = members.GetHeader().GetClusterId()
		return nil
	}

	return errors.Trace(errFailInitClusterID)
}

func (c *client) getMembers(ctx context.Context) (*pdpb.GetMembersResponse, error) {
	members, err := c.pdClient().GetMembers(ctx, &pdpb.GetMembersRequest{})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return members, nil
}

func (c *client) createConn() (*grpc.ClientConn, error) {
	cc, err := grpc.Dial(strings.TrimPrefix(c.url, "http://"), grpc.WithInsecure())
	if err != nil {
		return nil, errors.Trace(err)
	}
	return cc, nil
}

func (c *client) createHeartbeatStream() (pdpb.PD_RegionHeartbeatClient, context.Context, context.CancelFunc) {
	var (
		stream pdpb.PD_RegionHeartbeatClient
		err    error
		cancel context.CancelFunc
		ctx    context.Context
	)
	for {
		ctx, cancel = context.WithCancel(c.ctx)
		stream, err = c.pdClient().RegionHeartbeat(ctx)
		if err != nil {
			log.Errorf("[%s][pd] create region heartbeat stream error: %v", c.tag, err)
			cancel()
			select {
			case <-time.After(time.Second):
				continue
			case <-c.ctx.Done():
				log.Info("cancel create stream loop")
				return nil, ctx, cancel
			}
		}
		break
	}
	return stream, ctx, cancel
}

func (c *client) heartbeatStreamLoop() {
	defer c.wg.Done()
	for {
		stream, ctx, cancel := c.createHeartbeatStream()
		if stream == nil {
			return
		}
		errCh := make(chan error, 1)
		wg := &sync.WaitGroup{}
		wg.Add(2)
		go c.reportRegionHeartbeat(ctx, stream, errCh, wg)
		go c.receiveRegionHeartbeat(ctx, stream, errCh, wg)
		select {
		case err := <-errCh:
			log.Warnf("[%s][pd] heartbeat stream get error: %s ", c.tag, err)
			cancel()
			time.Sleep(time.Second)
		case <-c.ctx.Done():
			log.Info("cancel heartbeat stream loop")
			return
		}
		wg.Wait()
	}
}

func (c *client) receiveRegionHeartbeat(ctx context.Context, stream pdpb.PD_RegionHeartbeatClient, errCh chan error, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		resp, err := stream.Recv()
		if err != nil {
			errCh <- err
			return
		}

		if h := c.heartbeatHandler.Load(); h != nil {
			h.(func(*pdpb.RegionHeartbeatResponse))(resp)
		}
	}
}

func (c *client) reportRegionHeartbeat(ctx context.Context, stream pdpb.PD_RegionHeartbeatClient, errCh chan error, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case request, ok := <-c.regionCh:
			if !ok {
				return
			}
			request.Header = c.requestHeader()
			err := stream.Send(request)
			if err != nil {
				errCh <- err
			}
		}
	}
}

func (c *client) Close() {
	c.cancel()
	c.wg.Wait()

	if err := c.clientConn.Close(); err != nil {
		log.Errorf("[%s][pd] failed close grpc clientConn: %v", c.tag, err)
	}
}

func (c *client) GetClusterID(context.Context) uint64 {
	return c.clusterID
}

func (c *client) AllocID(ctx context.Context) (uint64, error) {
	ctx, cancel := context.WithTimeout(ctx, pdTimeout)
	resp, err := c.pdClient().AllocID(ctx, &pdpb.AllocIDRequest{
		Header: c.requestHeader(),
	})
	cancel()
	if err != nil {
		return 0, err
	}
	return resp.GetId(), nil
}

func (c *client) Bootstrap(ctx context.Context, store *metapb.Store, region *metapb.Region) (*pdpb.BootstrapResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, pdTimeout)
	res, err := c.pdClient().Bootstrap(ctx, &pdpb.BootstrapRequest{
		Header: c.requestHeader(),
		Store:  store,
		Region: region,
	})
	cancel()
	return res, err
}

func (c *client) IsBootstrapped(ctx context.Context) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, pdTimeout)
	resp, err := c.pdClient().IsBootstrapped(ctx, &pdpb.IsBootstrappedRequest{Header: c.requestHeader()})
	cancel()
	if err != nil {
		return false, err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return false, errors.New(herr.String())
	}
	return resp.Bootstrapped, nil
}

func (c *client) PutStore(ctx context.Context, store *metapb.Store) error {
	ctx, cancel := context.WithTimeout(ctx, pdTimeout)
	resp, err := c.pdClient().PutStore(ctx, &pdpb.PutStoreRequest{
		Header: c.requestHeader(),
		Store:  store,
	})
	cancel()
	if err != nil {
		return err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return errors.New(herr.String())
	}
	return nil
}

func (c *client) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	ctx, cancel := context.WithTimeout(ctx, pdTimeout)
	resp, err := c.pdClient().GetStore(ctx, &pdpb.GetStoreRequest{
		Header:  c.requestHeader(),
		StoreId: storeID,
	})
	cancel()
	if err != nil {
		return nil, err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return nil, errors.New(herr.String())
	}
	return resp.Store, nil
}

func (c *client) GetAllStores(ctx context.Context, excludeTombstone bool) ([]*metapb.Store, error) {
	ctx, cancel := context.WithTimeout(ctx, pdTimeout)
	resp, err := c.pdClient().GetAllStores(ctx, &pdpb.GetAllStoresRequest{
		Header:                 c.requestHeader(),
		ExcludeTombstoneStores: excludeTombstone,
	})
	cancel()
	if err != nil {
		return nil, err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return nil, errors.New(herr.String())
	}
	return resp.Stores, nil
}

func (c *client) GetClusterConfig(ctx context.Context) (*metapb.Cluster, error) {
	ctx, cancel := context.WithTimeout(ctx, pdTimeout)
	resp, err := c.pdClient().GetClusterConfig(ctx, &pdpb.GetClusterConfigRequest{
		Header: c.requestHeader(),
	})
	cancel()
	if err != nil {
		return nil, err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return nil, errors.New(herr.String())
	}
	return resp.Cluster, nil
}

func (c *client) GetRegion(ctx context.Context, key []byte) (*metapb.Region, error) {
	ctx, cancel := context.WithTimeout(ctx, pdTimeout)
	resp, err := c.pdClient().GetRegion(ctx, &pdpb.GetRegionRequest{
		Header:    c.requestHeader(),
		RegionKey: key,
	})
	cancel()
	if err != nil {
		return nil, err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return nil, errors.New(herr.String())
	}
	return resp.Region, nil
}

func (c *client) GetRegionByID(ctx context.Context, regionID uint64) (*metapb.Region, error) {
	ctx, cancel := context.WithTimeout(ctx, pdTimeout)
	resp, err := c.pdClient().GetRegionByID(ctx, &pdpb.GetRegionByIDRequest{
		Header:   c.requestHeader(),
		RegionId: regionID,
	})
	cancel()
	if err != nil {
		return nil, err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return nil, errors.New(herr.String())
	}
	return resp.Region, nil
}

func (c *client) AskSplit(ctx context.Context, region *metapb.Region) (*pdpb.AskSplitResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, pdTimeout)
	resp, err := c.pdClient().AskSplit(ctx, &pdpb.AskSplitRequest{
		Header: c.requestHeader(),
		Region: region,
	})
	cancel()
	if err != nil {
		return nil, err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return nil, errors.New(herr.String())
	}
	return resp, nil
}

func (c *client) AskBatchSplit(ctx context.Context, region *metapb.Region, count int) (*pdpb.AskBatchSplitResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, pdTimeout)
	resp, err := c.pdClient().AskBatchSplit(ctx, &pdpb.AskBatchSplitRequest{
		Header:     c.requestHeader(),
		Region:     region,
		SplitCount: uint32(count),
	})
	cancel()
	if err != nil {
		return nil, err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return nil, errors.New(herr.String())
	}
	return resp, nil
}

func (c *client) ReportBatchSplit(ctx context.Context, regions []*metapb.Region) error {
	ctx, cancel := context.WithTimeout(ctx, pdTimeout)
	resp, err := c.pdClient().ReportBatchSplit(ctx, &pdpb.ReportBatchSplitRequest{
		Header:  c.requestHeader(),
		Regions: regions,
	})
	cancel()
	if err != nil {
		return err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return errors.New(herr.String())
	}
	return nil
}

func (c *client) GetGCSafePoint(ctx context.Context) (uint64, error) {
	ctx, cancel := context.WithTimeout(ctx, pdTimeout)
	resp, err := c.pdClient().GetGCSafePoint(ctx, &pdpb.GetGCSafePointRequest{
		Header: c.requestHeader(),
	})
	cancel()
	if err != nil {
		return 0, err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return 0, errors.New(herr.String())
	}
	return resp.SafePoint, nil
}

func (c *client) StoreHeartbeat(ctx context.Context, stats *pdpb.StoreStats) error {
	ctx, cancel := context.WithTimeout(ctx, pdTimeout)
	resp, err := c.pdClient().StoreHeartbeat(ctx, &pdpb.StoreHeartbeatRequest{
		Header: c.requestHeader(),
		Stats:  stats,
	})
	cancel()
	if err != nil {
		return err
	}
	if herr := resp.Header.GetError(); herr != nil {
		return errors.New(herr.String())
	}
	return nil
}

func (c *client) ReportRegion(request *pdpb.RegionHeartbeatRequest) {
	c.regionCh <- request
}

func (c *client) SetRegionHeartbeatResponseHandler(h func(*pdpb.RegionHeartbeatResponse)) {
	if h == nil {
		h = func(*pdpb.RegionHeartbeatResponse) {}
	}
	c.heartbeatHandler.Store(h)
}

func (c *client) requestHeader() *pdpb.RequestHeader {
	return &pdpb.RequestHeader{
		ClusterId: c.clusterID,
	}
}
