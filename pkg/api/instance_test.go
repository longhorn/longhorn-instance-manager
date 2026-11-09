package api

import (
	"reflect"
	"testing"

	rpc "github.com/longhorn/types/pkg/generated/imrpc"
)

func TestReservationStateRPCRoundTrip(t *testing.T) {
	if RPCToReservationState(nil) != nil || ReservationStateToRPC(nil) != nil {
		t.Fatal("expected a nil state to stay nil")
	}

	state := &ReservationState{
		Epoch:        3,
		Type:         3,
		HolderKey:    0xAAAA,
		HolderHostID: "0f2a8bc4-1d9e-4b5c-8a7f-3e6d2c1b0a99",
		Registrants: []ReservationRegistrant{
			{Key: 0xAAAA, HostID: "0f2a8bc4-1d9e-4b5c-8a7f-3e6d2c1b0a99"},
			{Key: 0xFFFFFFFFFFFFFFFF, HostID: "11111111-2222-3333-4444-555555555555"},
		},
		VolumeUID: "6b1f3c2a-9d8e-4f7a-b6c5-1a2b3c4d5e6f",
	}
	got := RPCToReservationState(ReservationStateToRPC(state))
	if !reflect.DeepEqual(got, state) {
		t.Fatalf("got %+v, want %+v", got, state)
	}

	status := RPCToInstanceStatus(&rpc.InstanceStatus{Reservation: ReservationStateToRPC(state)})
	if !reflect.DeepEqual(status.Reservation, state) {
		t.Fatalf("got instance status reservation %+v, want %+v", status.Reservation, state)
	}
	if RPCToInstanceStatus(&rpc.InstanceStatus{}).Reservation != nil {
		t.Fatal("expected an unknown reservation state to stay nil")
	}
}
