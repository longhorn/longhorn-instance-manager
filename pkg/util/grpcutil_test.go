package util

import (
	"testing"
)

func Test_parseEndpoint(t *testing.T) {
	type args struct {
		ep string
	}
	tests := []struct {
		name        string
		args        args
		wantProto   string
		wantAddress string
		wantErr     bool
	}{
		{name: "testEndpointUnix", args: args{ep: "unix:///tmp/test.sock"}, wantProto: "unix", wantAddress: "/tmp/test.sock", wantErr: false},
		{name: "testEndpointTcp", args: args{ep: "tcp://127.0.0.1:8500"}, wantProto: "tcp", wantAddress: "127.0.0.1:8500", wantErr: false},
		{name: "testEndpointProtoMissingFallback", args: args{ep: "localhost:8500"}, wantProto: "tcp", wantAddress: "localhost:8500", wantErr: false},
		{name: "testEndpointProtoUnsupported", args: args{ep: "unsupported://127.0.0.1:8500"}, wantProto: "", wantAddress: "", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotProto, gotAddress, err := parseEndpoint(tt.args.ep)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseEndpoint() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotProto != tt.wantProto {
				t.Errorf("parseEndpoint() gotProto = %v, want %v", gotProto, tt.wantProto)
			}
			if gotAddress != tt.wantAddress {
				t.Errorf("parseEndpoint() gotAddress = %v, want %v", gotAddress, tt.wantAddress)
			}
		})
	}
}
