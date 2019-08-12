// Copyright (C) 2019 Noel R. Cower. All rights reserved.
// Use of this source code is governed by a BSD-style lciense that can be found
// in the COPYING file.

// Package srv is a simple package for looking up and dialing hosts described in SRV records.
package srv // import "go.spiff.io/srv"

import (
	"context"
	"errors"
	"math/rand"
	"net"
	"strconv"

	"golang.org/x/xerrors"
)

// GetNetworkFunc is a callback used by Dialer.
// Given a service and proto (including _s using standard names), the function should return
// a network supported by net.Dial, such as "tcp" or "udp". If the names aren't recognized, it
// should return an empty string.
type GetNetworkFunc func(service, proto, name string) string

// DialFunc is any dial function compatible with (*net.Dialer).DialContext.
type DialFunc func(ctx context.Context, network, address string) (net.Conn, error)

// ErrNoAddrs is returned if no names were returned in a request for SRV records.
var ErrNoAddrs = errors.New("no addresses found")

// Resolver is anything that performs DNS lookups for SRV records.
// This is made to be compatible with *net.Resolver.
type Resolver interface {
	LookupSRV(ctx context.Context, service, proto, name string) (cname string, addrs []*net.SRV, err error)
}

var _ Resolver = (*net.Resolver)(nil)

// Addr contains a network (for use in net.Dial) and a SRV record.
// It is not a fully-resolved address.
type Addr struct {
	Net string
	SRV *net.SRV
}

var _ net.Addr = (*Addr)(nil)

func (a *Addr) String() string {
	return a.SRV.Target + ":" + strconv.Itoa(int(a.SRV.Port))
}

func (a *Addr) Network() string {
	return a.Net
}

// Dialer looks up services in using SRV records and dials host:port combinations described by SRV
// records.
type Dialer struct {
	DialFunc   DialFunc
	Resolver   Resolver
	GetNetwork GetNetworkFunc
	RandInt    func(int) int
}

var zeroDialer *Dialer

func (d *Dialer) resolver() Resolver {
	if d == nil {
		return net.DefaultResolver
	}
	return d.Resolver
}

func (d *Dialer) randInt(n int) int {
	gen := rand.Intn
	if d != nil && d.RandInt != nil {
		gen = d.RandInt
	}
	i := gen(n)
	if i >= n {
		return n - 1
	} else if i < 0 {
		return 0
	}
	return i
}

func (d *Dialer) determineNetwork(service, proto, name string) (network string) {
	if d != nil && d.GetNetwork != nil {
		network = d.GetNetwork(service, proto, name)
		if network != "" {
			return network
		}
	}
	// Unless UDP is requested, assume TCP
	network = "tcp"
	if proto == "_udp" {
		network = "udp"
	}
	return network
}

// LookupAddrs looks up SRV records.
// See (*Dialer).LookupAddrs for more information, as this calls it on a default Dialer.
func LookupAddrs(ctx context.Context, service, proto, name string, limit int) ([]*Addr, error) {
	return zeroDialer.LookupAddrs(ctx, service, proto, name, limit)
}

// LookupAddrs looks up SRV records for the given service, proto, and name and returns up to limit
// addresses for them.
func (d *Dialer) LookupAddrs(ctx context.Context, service, proto, name string, limit int) ([]*Addr, error) {
	resolver := d.resolver()
	_, records, err := resolver.LookupSRV(ctx, service, proto, name)
	if err != nil {
		return nil, xerrors.Errorf(
			"lookup error for service=%q proto=%q name=%q: %v",
			service, proto, name, err,
		)
	}

	numRecords := len(records)
	if numRecords == 0 {
		return nil, xerrors.Errorf(
			"unable to look up service=%q proto=%q name=%q: %v",
			service, proto, name, ErrNoAddrs,
		)
	}
	if limit <= 0 || limit > numRecords {
		limit = numRecords
	}

	network := d.determineNetwork(service, proto, name)
	addrs := make([]*Addr, limit)
	for i, r := range records[:limit] {
		addrs[i] = &Addr{
			Net: network,
			SRV: r,
		}
	}
	return addrs, nil
}

func (d *Dialer) dialer() DialFunc {
	if d == nil {
		return (*net.Dialer)(nil).DialContext
	}
	return d.DialFunc
}

// Dial is a convenience function for DialContext(context.Background, network, address).
// See DialContext for more information.
func Dial(network, address string) (net.Conn, error) {
	return zeroDialer.DialContext(context.Background(), network, address)
}

// Dial is a convenience function for DialContext(context.Background, network, address).
// See DialContext for more information.
func (d *Dialer) Dial(network, address string) (net.Conn, error) {
	return d.DialContext(context.Background(), network, address)
}

// DialContext looks up SRV records for the address and dials a randomly selected host and port from
// the returned records.
func DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return zeroDialer.DialContext(ctx, network, address)
}

// DialContext looks up SRV records for the address and dials a randomly selected host and port from
// the returned records.
//
// Unlike DialSRV, if no SRV records are found for the given address, it will attempt to dial the
// address normally.
func (d *Dialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	addrs, err := d.LookupAddrs(ctx, "", "", address, 0)
	if xerrors.Is(err, ErrNoAddrs) {
		return d.dialer()(ctx, network, address)
	} else if err != nil {
		return nil, err
	}
	i := d.randInt(len(addrs))
	addr := addrs[i]
	if addrNet := addr.Network(); addrNet != network {
		return nil, xerrors.Errorf(
			"requested network and SRV network do not match: Dial(%q, ...) != SRV(%q)",
			network, addrNet,
		)
	}
	return d.dialer()(ctx, network, addr.String())
}

// DialSRV looks up SRV records for the given service, proto, and name and dials a randomly selected
// host and port from the returned records.
func DialSRV(ctx context.Context, service, proto, name string) (net.Conn, error) {
	return zeroDialer.DialSRV(ctx, service, proto, name)
}

// DialSRV looks up SRV records for the given service, proto, and name and dials a randomly selected
// host and port from the returned records.
func (d *Dialer) DialSRV(ctx context.Context, service, proto, name string) (net.Conn, error) {
	addrs, err := d.LookupAddrs(ctx, service, proto, name, 0)
	if err != nil {
		return nil, err
	}
	i := d.randInt(len(addrs))
	addr := addrs[i]
	return d.dialer()(ctx, addr.Network(), addr.String())
}
