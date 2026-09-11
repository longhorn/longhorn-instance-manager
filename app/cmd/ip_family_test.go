package cmd

import (
	"testing"

	commonnet "github.com/longhorn/go-common-libs/net"
)

func TestParseIPFamily(t *testing.T) {
	tests := []struct {
		name   string
		value  string
		family commonnet.IPFamily
		valid  bool
	}{
		{name: "empty", value: "", family: commonnet.IPFamilyUnspecified, valid: true},
		{name: "ipv4", value: "ipv4", family: commonnet.IPFamilyIPv4, valid: true},
		{name: "ipv6", value: "ipv6", family: commonnet.IPFamilyIPv6, valid: true},
		{name: "uppercase ipv4", value: "IPv4", valid: false},
		{name: "uppercase ipv6", value: "IPv6", valid: false},
		{name: "unknown", value: "ipv3", valid: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			family, err := parseIPFamily(test.value)
			if test.valid {
				if err != nil {
					t.Fatalf("parseIPFamily(%q) returned an error: %v", test.value, err)
				}
				if family != test.family {
					t.Fatalf("parseIPFamily(%q) = %q, want %q", test.value, family, test.family)
				}
				return
			}

			if err == nil {
				t.Fatalf("parseIPFamily(%q) succeeded, want an error", test.value)
			}
			if family != commonnet.IPFamilyUnspecified {
				t.Fatalf("parseIPFamily(%q) returned family %q on error", test.value, family)
			}
		})
	}
}
