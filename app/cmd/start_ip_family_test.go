package cmd

import (
	"bytes"
	"context"
	"net"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/longhorn/longhorn-instance-manager/pkg/types"
)

func TestStartCmdRemovesIPFamilyFlagAndHelp(t *testing.T) {
	cmd := StartCmd()
	for _, flag := range cmd.Flags {
		for _, name := range flag.Names() {
			if name == "ip-family" {
				t.Fatal("daemon command must not expose the removed --ip-family flag")
			}
		}
	}

	var output bytes.Buffer
	cmd.Writer = &output
	cmd.ErrWriter = &output
	if err := cmd.Run(context.Background(), []string{"instance-manager", "--help"}); err != nil {
		t.Fatalf("daemon --help returned an error: %v", err)
	}
	if strings.Contains(output.String(), "ip-family") {
		t.Fatalf("daemon help still mentions the removed --ip-family flag:\n%s", output.String())
	}
}

func TestStartCmdRejectsRemovedIPFamilyFlag(t *testing.T) {
	cmd := StartCmd()
	err := cmd.Run(context.Background(), []string{"instance-manager", "--ip-family=ipv6"})
	if err == nil {
		t.Fatal("daemon must reject the removed --ip-family flag")
	}
	if !strings.Contains(err.Error(), "flag provided but not defined") || !strings.Contains(err.Error(), "ip-family") {
		t.Fatalf("daemon rejected --ip-family with the wrong error: %v", err)
	}
}

func TestGetServiceAddressesIsFamilyNeutral(t *testing.T) {
	tests := []struct {
		name   string
		listen string
		want   map[string]string
	}{
		{
			name:   "wildcard",
			listen: ":8500",
			want: map[string]string{
				types.ProcessManagerGrpcService: ":8500",
				types.ProxyGRPCService:          ":8501",
				types.DiskGrpcService:           ":8502",
				types.InstanceGrpcService:       ":8503",
				types.SpdkGrpcService:           ":8504",
			},
		},
		{
			name:   "ipv4 kubernetes address",
			listen: "10.42.3.7:8500",
			want: map[string]string{
				types.ProcessManagerGrpcService: "10.42.3.7:8500",
				types.ProxyGRPCService:          "10.42.3.7:8501",
				types.DiskGrpcService:           "10.42.3.7:8502",
				types.InstanceGrpcService:       "10.42.3.7:8503",
				types.SpdkGrpcService:           "10.42.3.7:8504",
			},
		},
		{
			name:   "ipv6 kubernetes address",
			listen: "[fd00:10:244::7]:8500",
			want: map[string]string{
				types.ProcessManagerGrpcService: "[fd00:10:244::7]:8500",
				types.ProxyGRPCService:          "[fd00:10:244::7]:8501",
				types.DiskGrpcService:           "[fd00:10:244::7]:8502",
				types.InstanceGrpcService:       "[fd00:10:244::7]:8503",
				types.SpdkGrpcService:           "[fd00:10:244::7]:8504",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := getServiceAddresses(tc.listen)
			if err != nil {
				t.Fatalf("getServiceAddresses(%q) returned an error: %v", tc.listen, err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("getServiceAddresses(%q) = %#v, want %#v", tc.listen, got, tc.want)
			}
		})
	}
}

func TestDerivedServiceAddressesAreReachable(t *testing.T) {
	tests := []struct {
		name string
		host string
		ipv6 bool
	}{
		{name: "wildcard", host: ""},
		{name: "ipv4 loopback", host: "127.0.0.1"},
		{name: "ipv6 loopback", host: "::1", ipv6: true},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			if tc.ipv6 {
				probe, err := net.Listen("tcp6", "[::1]:0")
				if err != nil {
					t.Skipf("IPv6 loopback is unavailable: %v", err)
				}
				_ = probe.Close()
			}

			probe, err := net.Listen("tcp4", "127.0.0.1:0")
			if err != nil {
				t.Fatalf("reserve a service port: %v", err)
			}
			basePort := probe.Addr().(*net.TCPAddr).Port
			_ = probe.Close()

			listen := net.JoinHostPort(tc.host, strconv.Itoa(basePort))
			addresses, err := getServiceAddresses(listen)
			if err != nil {
				t.Fatalf("getServiceAddresses(%q) returned an error: %v", listen, err)
			}

			listeners := make([]net.Listener, 0, len(addresses))
			defer func() {
				for _, listener := range listeners {
					_ = listener.Close()
				}
			}()
			for service, address := range addresses {
				listener, err := net.Listen("tcp", address)
				if err != nil {
					t.Fatalf("net.Listen for %s at %s: %v", service, address, err)
				}
				listeners = append(listeners, listener)
			}

			for service, address := range addresses {
				clientAddress := toClientAddress(address)
				if tc.host == "" {
					_, port, err := net.SplitHostPort(address)
					if err != nil {
						t.Fatalf("split derived wildcard address %q: %v", address, err)
					}
					wantClientAddress := net.JoinHostPort("127.0.0.1", port)
					if clientAddress != wantClientAddress {
						t.Fatalf("toClientAddress(%q) = %q, want %q", address, clientAddress, wantClientAddress)
					}
				}
				conn, err := net.DialTimeout("tcp", clientAddress, time.Second)
				if err != nil {
					t.Fatalf("dial derived %s address %s via %s: %v", service, address, clientAddress, err)
				}
				_ = conn.Close()
			}
		})
	}
}
