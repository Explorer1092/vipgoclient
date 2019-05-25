// Copyright 2018 Comcast Cable Communications Management, LLC
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
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/jiazhai/vipgoclient/auth"
	"github.com/jiazhai/vipgoclient/frame"
)

// NewConn creates a connection to given pulsar server.
func NewConn(addr string, timeout time.Duration, tlsCfg *tls.Config, authMethod auth.Authentication) (*Conn, error) {
	var c net.Conn
	var err error
	if strings.HasPrefix(addr, "pulsar+ssl://") {
		c, err = newTLSConn(addr, timeout, tlsCfg, authMethod)
	} else {
		c, err = newTCPConn(addr, timeout)
	}
	if err != nil {
		return nil, err
	}
	return &Conn{
		NetConn: c,
		Closedc: make(chan struct{}),
	}, nil
}

// newTCPConn creates a core using a TCPv4 connection to the given
// (pulsar server) address.
func newTCPConn(addr string, timeout time.Duration) (net.Conn, error) {
	addr = strings.TrimPrefix(addr, "pulsar://")

	d := net.Dialer{
		DualStack: false,
		Timeout:   timeout,
	}
	c, err := d.Dial("tcp4", addr)
	if err != nil {
		return nil, err
	}

	return c, nil
}

// newTLSConn creates a core using a TCPv4+TLS connection to the given
// (pulsar server) address.
func newTLSConn(addr string, timeout time.Duration, tlsCfg *tls.Config, authMethod auth.Authentication) (net.Conn, error) {
	addr = strings.TrimPrefix(addr, "pulsar+ssl://")

	d := net.Dialer{
		DualStack: false,
		Timeout:   timeout,
	}

	if authMethod != nil && authMethod.GetAuthName() == auth.AuthTLS {
		if tlsAuth, ok := (authMethod).(*auth.TLSAuthentication); ok {
			tlsAuth.ConfigClientAuth(tlsCfg)
		} else {
			return nil, errors.New("invalid tls auth")
		}
	}

	c, err := tls.DialWithDialer(&d, "tcp4", addr, tlsCfg)
	if err != nil {
		return nil, err
	}

	return c, nil
}

// Conn is responsible for writing and reading
// Frames to and from the underlying connection (r and w).
type Conn struct {
	NetConn net.Conn
	Wmu     sync.Mutex // protects w to ensure frames aren't interleaved

	Cmu      sync.Mutex // protects following
	IsClosed bool
	Closedc  chan struct{}
}

// Close closes the underlaying connection.
// This will cause read() to unblock and return
// an error. It will also cause the closed channel
// to unblock.
func (c *Conn) Close() error {
	c.Cmu.Lock()
	defer c.Cmu.Unlock()

	if c.IsClosed {
		return nil
	}

	err := c.NetConn.Close()
	close(c.Closedc)
	c.IsClosed = true

	return err
}

// Closed returns a channel that will unblock
// when the connection has been closed and is no
// longer usable.
func (c *Conn) Closed() <-chan struct{} {
	return c.Closedc
}

// Read blocks while it reads from r until an error occurs.
// It passes all frames to the provided handler, sequentially
// and from the same goroutine as called with. Any error encountered
// will close the connection. Also if close() is called,
// read() will unblock. Once read returns, the core should
// be considered unusable.
func (c *Conn) Read(frameHandler func(f frame.Frame)) error {
	for {
		var f frame.Frame
		if err := f.Decode(c.NetConn); err != nil {
			// It's very possible that the connection is already closed at this
			// point, since any connection closed errors would bubble up
			// from Decode. But just in case it's a decode error (bad data for example),
			// we attempt to close the connection. Any error is ignored
			// since the Decode error is the primary one.
			_ = c.Close()
			return err
		}
		frameHandler(f)
	}
}

// Write writes a frame.It is safe to use concurrently.
func (c *Conn) Write(ctx context.Context, f *frame.Frame) error {
	var t time.Time
	if ctx != nil {
		if deadline, ok := ctx.Deadline(); ok {
			t = deadline
		}
	}
	return c.writeFrame(t, f)
}

var bufPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 0, frame.MaxFrameSize))
	},
}

// writeFrame encodes the given frame and writes
// it to the wire in a thread-safe manner.
func (c *Conn) writeFrame(deadline time.Time, f *frame.Frame) error {
	b := bufPool.Get().(*bytes.Buffer)
	defer bufPool.Put(b)
	b.Reset()

	if err := f.Encode(b); err != nil {
		return err
	}

	c.Wmu.Lock()
	err := c.NetConn.SetWriteDeadline(deadline)
	if err != nil {
		return err
	}
	_, err = b.WriteTo(c.NetConn)
	c.Wmu.Unlock()

	return err
}
