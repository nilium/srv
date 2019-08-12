// Copyright (C) 2019 Noel R. Cower. All rights reserved.
// Use of this source code is governed by a BSD-style lciense that can be found
// in the COPYING file.

// TODO(ncower): Implement graceful shutdown of the proxy server.

// Program srvproxy is a simply TCP proxy that accepts connections and proxies
// them to an upstream server described by SRV records.
//
// Example:
//
//      $ srvproxy -v tcp://127.0.0.1:8022/_db._tcp.service.dc.consul
//
// The above will start srvproxy in verbose mode (log connections) and accept
// TCP connections for 127.0.0.1:8022. Once a connection is received, it
// requests SRV records for '_db._tcp.service.consul', creates a connection to
// one of the upstream servers, and proxies data between the two connections.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.spiff.io/srv"
)

var verbose bool
var defaultBufferSize = 16384

func vlogf(format string, args ...interface{}) {
	if !verbose {
		return
	}
	msg := fmt.Sprintf(format, args...)
	_ = log.Output(2, msg)
}

type deadlineListener interface {
	net.Listener
	SetDeadline(time.Time) error
}

type server struct {
	URL *url.URL

	network string
	address string
	target  string

	limit int

	bufferSize int
}

func newServer(urlstr string) (*server, error) {
	u, err := url.Parse(urlstr)
	if err != nil {
		return nil, err
	}
	switch u.Scheme {
	case "tcp", "tcp4", "tcp6", "unix":
	default:
		return nil, fmt.Errorf("cannot proxy %q protocol", u.Scheme)
	}

	if u.Host == "" {
		return nil, errors.New("no hostname found in URL")
	}

	if u.Port() == "" {
		return nil, errors.New("no port found in URL")
	}

	if u.Path == "" || u.Path == "/" {
		return nil, errors.New("URL path must be a domain name")
	}

	// Drop the leading slash
	domain := strings.TrimPrefix(u.Path, "/")
	if strings.IndexByte(domain, '/') != -1 {
		return nil, errors.New("URL path may not contain anything other than a leading slash")
	}

	params := u.Query()

	var limit int
	if limitStr := params.Get("limit"); limitStr != "" {
		limit, err = strconv.Atoi(limitStr)
		if err != nil {
			return nil, fmt.Errorf("unable to parse limit (limit=%q): %v", limitStr, err)
		} else if limit < 0 {
			return nil, fmt.Errorf("limit must be >= 0")
		}
	}

	bufferSize := defaultBufferSize
	if bufferStr := params.Get("buffer"); bufferStr != "" {
		bufferSize, err = strconv.Atoi(bufferStr)
		if err != nil {
			return nil, fmt.Errorf("unable to parse buffer size (buffer=%q): %v", bufferStr, err)
		} else if bufferSize == 0 {
			// reset
			bufferSize = defaultBufferSize
		} else if bufferSize < 0 {
			return nil, fmt.Errorf("buffer must be >= 0")
		}
	}

	u.RawQuery = ""
	u.Fragment = ""
	u.Opaque = ""

	return &server{
		URL:        u,
		network:    u.Scheme,
		address:    u.Host,
		target:     domain,
		limit:      limit,
		bufferSize: bufferSize,
	}, nil
}

func (s *server) dial(ctx context.Context) (net.Conn, error) {
	addrs, err := srv.LookupAddrs(ctx, "", "", s.target, s.limit)
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve upstream server: %v", err)
	}

	i := rand.Intn(len(addrs))
	addr := addrs[i]

	network := addr.Network()
	switch network {
	case "tcp", "tcp4", "tcp6":
	default:
		return nil, fmt.Errorf("usupported protocol %q for address %v; cannot dial upstream server", network, addr)
	}
	return net.Dial(network, addr.String())
}

func (s *server) listenAndServe(ctx context.Context) error {
	listener, err := net.Listen(s.network, s.address)
	if err != nil {
		return fmt.Errorf("error binding to %s(%s): %v", s.network, s.address, err)
	}
	defer listener.Close()

	dl, _ := listener.(deadlineListener)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		if dl != nil {
			t := time.Now().Add(time.Second)
			if err := dl.SetDeadline(t); err != nil {
				return fmt.Errorf("unable to set accept deadline to %v: %v", t, err)
			}
		}

		conn, err := listener.Accept()
		switch ne := err.(type) {
		case nil:
		case net.Error:
			if ne.Temporary() || ne.Timeout() {
				continue
			}
			return err
		default:
			return err
		}
		go s.serve(ctx, conn)
	}
}

func (s *server) serve(ctx context.Context, clientConn net.Conn) {
	defer clientConn.Close()
	clientTCP := clientConn.(*net.TCPConn)
	clientAddr := clientTCP.RemoteAddr()

	proxyConn, err := s.dial(ctx)
	if err != nil {
		log.Printf("(%v) unable to connect to upstream server: %v", s.URL, err)
		return
	}
	defer proxyConn.Close()
	proxyTCP := proxyConn.(*net.TCPConn)
	serverAddr := proxyTCP.RemoteAddr()

	vlogf("(%v) accepted connection: %v -> %v", s.URL, clientAddr, serverAddr)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	errors := make(chan error, 1)
	wg.Add(2)
	go func() { defer wg.Done(); errors <- s.pipe(ctx, clientTCP, proxyTCP) }()
	go func() { defer wg.Done(); errors <- s.pipe(ctx, proxyTCP, clientTCP) }()
	defer wg.Wait()

	err = <-errors
	switch err {
	case nil:
	case context.Canceled:
	case io.EOF:
	default:
		vlogf("(%v) connection error: %v", s.URL, err)
		return
	}
	vlogf("(%v) connection closed: %v -> %v", s.URL, clientAddr, serverAddr)
}

func (s *server) pipe(ctx context.Context, dst, src *net.TCPConn) error {
	defer verboseClosef(dst.CloseWrite, "(%v) error closing write end of connection %v", s.URL, dst.RemoteAddr())
	defer verboseClosef(src.CloseRead, "(%v) error closing write end of connection %v", s.URL, src.RemoteAddr())
	buf := make([]byte, s.bufferSize)
	_, err := io.CopyBuffer(dst, src, buf)
	return err
}

func verboseClosef(fn func() error, format string, args ...interface{}) {
	if err := fn(); err != nil {
		vlogf("%s: %v", fmt.Sprintf(format, args...), err)
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)

	flag.IntVar(&defaultBufferSize, "b", defaultBufferSize, "Default read/write buffer `size` (bytes)")
	flag.BoolVar(&verbose, "v", false, "Verbose logging")
	flag.Parse()

	if defaultBufferSize <= 0 {
		log.Fatalf("Invalid buffer size %d: must be > 1", defaultBufferSize)
	}

	servers := []*server{}
	for _, u := range flag.Args() {
		sv, err := newServer(u)
		if err != nil {
			log.Fatalf("Error configuring server %q: %v", u, err)
		}
		servers = append(servers, sv)
	}

	for _, sv := range servers {
		sv := sv
		go func() {
			if err := sv.listenAndServe(context.Background()); err != nil {
				log.Fatalf("Fatal error serving %v: %v", sv.URL, err)
			}
			log.Fatalf("Server %v unexpectedly closed", sv.URL)
		}()
	}

	select {}
}
