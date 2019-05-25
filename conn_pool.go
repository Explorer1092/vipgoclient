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
	"crypto/tls"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/jiazhai/vipgoclient/auth"
	"github.com/jiazhai/vipgoclient/frame"
)

// ConnectionPool provides management abilities of *ConnWrapper which
// can be shared by different clients.
type ConnectionPool struct {
	// Timeout timeout for create a new connection
	Timeout time.Duration
	// AuthMethod auth interface
	AuthMethod auth.Authentication

	pool   map[string]*ConnWrapper
	tlsCfg *tls.Config
	mu     sync.Mutex
}

// Close close all connection
func (cp *ConnectionPool) Close() error {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	var err error
	for _, c := range cp.pool {
		err = c.Close()
	}
	return err
}

// GetConnection return a *ConnWrapper by addr. A new one will be created
// if not exists.
func (cp *ConnectionPool) GetConnection(logicalAddress, physicalAddress string) (*ConnWrapper, error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()
	if c, ok := cp.pool[logicalAddress]; ok {
		if !c.IsClose() {
			return c, nil
		}
		delete(cp.pool, logicalAddress)
	}

	c, err := cp.newConnWrapper(logicalAddress, physicalAddress)
	if err != nil {
		return nil, err
	}
	if cp.pool == nil {
		cp.pool = make(map[string]*ConnWrapper)
	}
	cp.pool[logicalAddress] = c
	return c, nil
}

func (cp *ConnectionPool) newConnWrapper(logicalAddress, physicalAddress string) (*ConnWrapper, error) {
	c, err := cp.newConnection(physicalAddress)
	if err != nil {
		return nil, err
	}
	cw := &ConnWrapper{
		C:               c,
		LogicalAddr:     logicalAddress,
		PhysicalAddr:    physicalAddress,
		RequestID:       &MonotonicID{ID: 0},
		Dispatcher:      frame.NewFrameDispatcher(),
		ConsumeHandlers: make(map[uint64]ConsumeHandler),
		ProduceHandlers: make(map[uint64]ProduceHandler),
	}

	err = cw.Start(cp.Timeout, cp.AuthMethod)
	if err != nil {
		cw.Close()
		return nil, err
	}
	return cw, nil
}

func (cp *ConnectionPool) newConnection(addr string) (*Conn, error) {
	var c net.Conn
	var err error
	if strings.HasPrefix(addr, "pulsar+ssl://") {
		c, err = newTLSConn(addr, cp.Timeout, cp.tlsCfg, cp.AuthMethod)
	} else {
		c, err = newTCPConn(addr, cp.Timeout)
	}
	if err != nil {
		return nil, err
	}
	return &Conn{
		NetConn: c,
		Closedc: make(chan struct{}),
	}, nil
}
