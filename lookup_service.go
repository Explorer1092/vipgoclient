// Copyright 2019 zhvala
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pulsar

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"

	"code.vipkid.com.cn/zhanghao1/go-pulsar-client/pkg/api"
)

type LookupData struct {
	BrokerServiceUrl       string
	BrokerServiceUrlTls    string
	ProxyThroughServiceUrl bool
}

// LookupService opaque interface that provides the ability to lookup topic
// and namespace infomation.
type LookupService interface {
	LookupTopic(ctx context.Context, topic string) (*LookupData, error)
	GetPartitions(ctx context.Context, topic string) (uint32, error)
	GetTopicsOfNamespace(ctx context.Context, namespace string) ([]string, error)
}

func NewLookupService(ctx context.Context, url string, tlsCfg *tls.Config, connPool *ConnectionPool) LookupService {
	if strings.HasPrefix(url, "http") {
		// not support http lookup yet
		return nil
	}
	return &BinaryLookupService{
		URL:      url,
		UseTLS:   strings.HasPrefix(url, "pulsar+ssl://"),
		ConnPool: connPool,
	}
}

// HTTPLookupService
type HTTPLookupService struct {
}

func (hls *HTTPLookupService) LookupTopic(ctx context.Context, topic string) (*api.CommandLookupTopicResponse, error) {
	return nil, nil
}

func (hls *HTTPLookupService) GetPartitions(ctx context.Context, topic string) (*api.CommandPartitionedTopicMetadataResponse, error) {
	return nil, nil
}

func (hls *HTTPLookupService) GetTopicsOfNamespace(ctx context.Context, namespace string) (*api.CommandGetTopicsOfNamespaceResponse, error) {
	return nil, nil
}

const (
	maxTopicLookupRedirects = 8
)

// BinaryLookupService
type BinaryLookupService struct {
	URL      string
	UseTLS   bool
	ConnPool *ConnectionPool
}

func (bls *BinaryLookupService) LookupTopic(ctx context.Context, topic string) (*LookupData, error) {
	// For initial lookup request, authoritative should == false
	var authoritative bool
	logicAddr, phyAddr := bls.URL, bls.URL

	for redirects := 0; redirects < maxTopicLookupRedirects; redirects++ {
		conn, err := bls.ConnPool.GetConnection(logicAddr, phyAddr)
		if err != nil {
			return nil, err
		}

		resp, err := conn.LookupTopic(ctx, topic, authoritative)
		if err != nil {
			return nil, err
		}

		// Response type can be:
		// - Connect
		// - Redirect
		// - Failed
		lookupType := resp.GetResponse()
		authoritative = resp.GetAuthoritative()

		switch lookupType {
		case api.CommandLookupTopicResponse_Redirect:
			// Repeat process, but with new broker address
			// Update configured address with address
			// provided in response
			if bls.UseTLS {
				logicAddr = resp.GetBrokerServiceUrlTls()
			} else {
				logicAddr = resp.GetBrokerServiceUrl()
			}

			// If ProxyThroughServiceUrl is true, then
			// the original address must be used for the physical
			// TCP connection. The lookup response address must then
			// be provided in the Connect command. But the broker service
			// address should be used by the pool as part of the lookup key.
			if resp.GetProxyThroughServiceUrl() {
				// what will happen when URL is http
				phyAddr = bls.URL
			} else {
				phyAddr = logicAddr
			}
		case api.CommandLookupTopicResponse_Failed:
			lookupErr := resp.GetError()
			return nil, fmt.Errorf("(%s) %s", lookupErr.String(), resp.GetMessage())
		case api.CommandLookupTopicResponse_Connect:
			return &LookupData{
				BrokerServiceUrl:       resp.GetBrokerServiceUrl(),
				BrokerServiceUrlTls:    resp.GetBrokerServiceUrlTls(),
				ProxyThroughServiceUrl: resp.GetProxyThroughServiceUrl(),
			}, nil
		}
	}

	return nil, errors.New("redirect over limit")
}

func (bls *BinaryLookupService) GetPartitions(ctx context.Context, topic string) (uint32, error) {
	conn, err := bls.ConnPool.GetConnection(bls.URL, bls.URL)
	if err != nil {
		return 0, err
	}
	resp, err := conn.PartitionedMetadata(ctx, topic)
	if err != nil {
		return 0, err
	}
	return resp.GetPartitions(), nil
}

func (bls *BinaryLookupService) GetTopicsOfNamespace(ctx context.Context, namespace string) ([]string, error) {
	conn, err := bls.ConnPool.GetConnection(bls.URL, bls.URL)
	if err != nil {
		return nil, err
	}
	resp, err := conn.TopicsOfNamespace(ctx, namespace)
	if err != nil {
		return nil, err
	}
	return resp.GetTopics(), nil
}
