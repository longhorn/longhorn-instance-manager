package client

import (
	"testing"

	rpc "github.com/longhorn/types/pkg/generated/imrpc"
)

func TestGetDataEngine(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{name: "v1", input: "v1", expected: rpc.DataEngine_DATA_ENGINE_V1.String()},
		{name: "v2", input: "v2", expected: rpc.DataEngine_DATA_ENGINE_V2.String()},
		{name: "local", input: "local", expected: rpc.DataEngine_DATA_ENGINE_LOCAL.String()},
		{name: "protobuf v1", input: "DATA_ENGINE_V1", expected: rpc.DataEngine_DATA_ENGINE_V1.String()},
		{name: "protobuf v2", input: "DATA_ENGINE_V2", expected: rpc.DataEngine_DATA_ENGINE_V2.String()},
		{name: "protobuf local", input: "DATA_ENGINE_LOCAL", expected: rpc.DataEngine_DATA_ENGINE_LOCAL.String()},
		{name: "empty", input: "", expected: ""},
		{name: "unknown", input: "v4", expected: ""},
		{name: "invalid v1 suffix", input: "invalid-v1", expected: ""},
		{name: "invalid v2 suffix", input: "invalid-v2", expected: ""},
		{name: "invalid local suffix", input: "invalid-local", expected: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := getDataEngine(test.input); got != test.expected {
				t.Fatalf("getDataEngine(%q) = %q, want %q", test.input, got, test.expected)
			}
		})
	}
}
