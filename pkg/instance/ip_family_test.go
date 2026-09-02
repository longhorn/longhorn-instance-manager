package instance

import (
	"context"
	"net"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	spdkapi "github.com/longhorn/longhorn-spdk-engine/pkg/api"
	rpc "github.com/longhorn/types/pkg/generated/imrpc"
	spdkrpc "github.com/longhorn/types/pkg/generated/spdkrpc"

	"github.com/longhorn/longhorn-instance-manager/pkg/client"
	"github.com/longhorn/longhorn-instance-manager/pkg/types"
)

type ipFamilyCaptureSPDKServer struct {
	spdkrpc.UnimplementedSPDKServiceServer

	mu               sync.Mutex
	engineRequests   []*spdkrpc.EngineCreateRequest
	frontendRequests []*spdkrpc.EngineFrontendCreateRequest
	replicaRequests  []*spdkrpc.ReplicaCreateRequest
}

func (s *ipFamilyCaptureSPDKServer) EngineCreate(_ context.Context, req *spdkrpc.EngineCreateRequest) (*spdkrpc.Engine, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.engineRequests = append(s.engineRequests, req)
	return &spdkrpc.Engine{Name: req.Name, IpFamily: req.IpFamily}, nil
}

func (s *ipFamilyCaptureSPDKServer) EngineFrontendCreate(_ context.Context, req *spdkrpc.EngineFrontendCreateRequest) (*spdkrpc.EngineFrontend, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.frontendRequests = append(s.frontendRequests, req)
	return &spdkrpc.EngineFrontend{Name: req.Name, IpFamily: req.IpFamily}, nil
}

func (s *ipFamilyCaptureSPDKServer) ReplicaCreate(_ context.Context, req *spdkrpc.ReplicaCreateRequest) (*spdkrpc.Replica, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.replicaRequests = append(s.replicaRequests, req)
	return &spdkrpc.Replica{Name: req.Name, IpFamily: req.IpFamily}, nil
}

func startIPFamilyCaptureSPDKServer(t *testing.T) (*ipFamilyCaptureSPDKServer, string, func()) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for fake SPDK service: %v", err)
	}
	server := grpc.NewServer()
	capture := &ipFamilyCaptureSPDKServer{}
	spdkrpc.RegisterSPDKServiceServer(server, capture)
	go func() {
		_ = server.Serve(listener)
	}()

	cleanup := func() {
		server.Stop()
		_ = listener.Close()
	}
	return capture, listener.Addr().String(), cleanup
}

type publicInstanceCaptureServer struct {
	rpc.UnimplementedInstanceServiceServer

	mu        sync.Mutex
	creates   map[string]*rpc.InstanceCreateRequest
	responses map[string]*rpc.InstanceResponse
}

func (s *publicInstanceCaptureServer) InstanceCreate(_ context.Context, req *rpc.InstanceCreateRequest) (*rpc.InstanceResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.creates[req.Spec.Name] = req
	response := &rpc.InstanceResponse{
		Spec: &rpc.InstanceSpec{
			Name:       req.Spec.Name,
			Type:       req.Spec.Type,
			DataEngine: req.Spec.DataEngine,
		},
		Status: &rpc.InstanceStatus{
			State:    types.ProcessStateRunning,
			IpFamily: req.Spec.IpFamily,
		},
	}
	s.responses[req.Spec.Name] = response
	return response, nil
}

func (s *publicInstanceCaptureServer) InstanceGet(_ context.Context, req *rpc.InstanceGetRequest) (*rpc.InstanceResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	response, ok := s.responses[req.Name]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "instance %s not found", req.Name)
	}
	return response, nil
}

func (s *publicInstanceCaptureServer) InstanceList(context.Context, *emptypb.Empty) (*rpc.InstanceListResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	responses := make(map[string]*rpc.InstanceResponse, len(s.responses))
	for name, response := range s.responses {
		responses[name] = response
	}
	return &rpc.InstanceListResponse{Instances: responses}, nil
}

func startPublicInstanceCaptureServer(t *testing.T) (*publicInstanceCaptureServer, string, func()) {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for fake Instance service: %v", err)
	}
	server := grpc.NewServer()
	capture := &publicInstanceCaptureServer{
		creates:   map[string]*rpc.InstanceCreateRequest{},
		responses: map[string]*rpc.InstanceResponse{},
	}
	rpc.RegisterInstanceServiceServer(server, capture)
	go func() {
		_ = server.Serve(listener)
	}()

	cleanup := func() {
		server.Stop()
		_ = listener.Close()
	}
	return capture, listener.Addr().String(), cleanup
}

func TestV2InstanceCreateAndStatusIPFamilyRoundTrip(t *testing.T) {
	capture, address, cleanup := startIPFamilyCaptureSPDKServer(t)
	defer cleanup()

	ops := V2DataEngineInstanceOps{spdkServiceAddress: address}
	families := []string{"", "ipv4", "ipv6"}
	for _, family := range families {
		family := family
		t.Run("engine/"+familyName(family), func(t *testing.T) {
			request := &rpc.InstanceCreateRequest{Spec: &rpc.InstanceSpec{
				Name:       "engine-" + familyName(family),
				Type:       types.InstanceTypeEngine,
				VolumeName: "volume-" + familyName(family),
				PortCount:  1,
				IpFamily:   family,
				DataEngine: rpc.DataEngine_DATA_ENGINE_V2,
				SpdkInstanceSpec: &rpc.SpdkInstanceSpec{
					ReplicaAddressMap: map[string]string{"replica": "127.0.0.1:10000"},
				},
			}}
			response, err := ops.InstanceCreate(request)
			if err != nil {
				t.Fatalf("engine create failed: %v", err)
			}
			capture.mu.Lock()
			gotRequest := capture.engineRequests[len(capture.engineRequests)-1]
			capture.mu.Unlock()
			if gotRequest.IpFamily != family {
				t.Fatalf("engine create family = %q, want %q", gotRequest.IpFamily, family)
			}
			if response.Status.GetIpFamily() != family {
				t.Fatalf("engine status family = %q, want %q", response.Status.GetIpFamily(), family)
			}
		})

		t.Run("engine-frontend/"+familyName(family), func(t *testing.T) {
			request := &rpc.InstanceCreateRequest{Spec: &rpc.InstanceSpec{
				Name:          "frontend-" + familyName(family),
				Type:          types.InstanceTypeEngineFrontend,
				VolumeName:    "volume-" + familyName(family),
				EngineName:    "engine-" + familyName(family),
				IpFamily:      family,
				DataEngine:    rpc.DataEngine_DATA_ENGINE_V2,
				TargetAddress: "127.0.0.1:10001",
				SpdkInstanceSpec: &rpc.SpdkInstanceSpec{
					Frontend: "ublk",
				},
			}}
			response, err := ops.InstanceCreate(request)
			if err != nil {
				t.Fatalf("engine frontend create failed: %v", err)
			}
			capture.mu.Lock()
			gotRequest := capture.frontendRequests[len(capture.frontendRequests)-1]
			capture.mu.Unlock()
			if gotRequest.IpFamily != family {
				t.Fatalf("engine frontend create family = %q, want %q", gotRequest.IpFamily, family)
			}
			if response.Status.GetIpFamily() != family {
				t.Fatalf("engine frontend status family = %q, want %q", response.Status.GetIpFamily(), family)
			}
		})

		t.Run("replica/"+familyName(family), func(t *testing.T) {
			request := &rpc.InstanceCreateRequest{Spec: &rpc.InstanceSpec{
				Name:       "replica-" + familyName(family),
				Type:       types.InstanceTypeReplica,
				IpFamily:   family,
				DataEngine: rpc.DataEngine_DATA_ENGINE_V2,
				SpdkInstanceSpec: &rpc.SpdkInstanceSpec{
					DiskName: "disk",
					DiskUuid: "disk-uuid",
				},
			}}
			response, err := ops.InstanceCreate(request)
			if err != nil {
				t.Fatalf("replica create failed: %v", err)
			}
			capture.mu.Lock()
			gotRequest := capture.replicaRequests[len(capture.replicaRequests)-1]
			capture.mu.Unlock()
			if gotRequest.IpFamily != family {
				t.Fatalf("replica create family = %q, want %q", gotRequest.IpFamily, family)
			}
			if response.Status.GetIpFamily() != family {
				t.Fatalf("replica status family = %q, want %q", response.Status.GetIpFamily(), family)
			}
		})
	}
}

func familyName(family string) string {
	if family == "" {
		return "unspecified"
	}
	return family
}

func TestUnsupportedInstanceResponsesRemainUnspecified(t *testing.T) {
	responses := map[string]*rpc.InstanceResponse{
		"v1 process": processResponseToInstanceResponse(&rpc.ProcessResponse{
			Spec:   &rpc.ProcessSpec{},
			Status: &rpc.ProcessStatus{},
		}, types.InstanceTypeEngine),
		"shard":       shardResponseToInstanceResponse(&spdkapi.Shard{}),
		"shard group": shardGroupResponseToInstanceResponse(&spdkrpc.ShardGroup{}),
	}
	for name, response := range responses {
		t.Run(name, func(t *testing.T) {
			if response.Spec.GetIpFamily() != "" {
				t.Fatalf("unsupported instance spec family = %q, want empty", response.Spec.GetIpFamily())
			}
			if response.Status.GetIpFamily() != "" {
				t.Fatalf("unsupported instance status family = %q, want empty", response.Status.GetIpFamily())
			}
		})
	}
}

func TestPublicInstanceClientIPFamilyRoundTrip(t *testing.T) {
	capture, address, cleanup := startPublicInstanceCaptureServer(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	publicClient, err := client.NewInstanceServiceClient(ctx, cancel, "tcp://"+address, nil)
	if err != nil {
		t.Fatalf("NewInstanceServiceClient returned an error: %v", err)
	}
	defer func() {
		if err := publicClient.Close(); err != nil {
			t.Errorf("failed to close public instance client: %v", err)
		}
	}()

	for _, instanceType := range []string{types.InstanceTypeEngine, types.InstanceTypeEngineFrontend, types.InstanceTypeReplica} {
		instanceType := instanceType
		for _, family := range []string{"", "ipv4", "ipv6"} {
			family := family
			t.Run(instanceType+"/"+familyName(family), func(t *testing.T) {
				name := instanceType + "-" + familyName(family)
				request := &client.InstanceCreateRequest{
					DataEngine:   "v2",
					Name:         name,
					InstanceType: instanceType,
					VolumeName:   "volume-" + familyName(family),
					Size:         4096,
				}
				switch instanceType {
				case types.InstanceTypeEngine:
					request.Engine = client.EngineCreateRequest{
						ReplicaAddressMap: map[string]string{"replica": "127.0.0.1:10000"},
						IPFamily:          family,
					}
				case types.InstanceTypeEngineFrontend:
					request.EngineFrontend = client.EngineFrontendCreateRequest{
						EngineName: "engine-" + familyName(family),
						IPFamily:   family,
					}
				case types.InstanceTypeReplica:
					request.Replica = client.ReplicaCreateRequest{
						DiskName: "disk",
						DiskUUID: "disk-uuid",
						IPFamily: family,
					}
				}

				created, err := publicClient.InstanceCreate(request)
				if err != nil {
					t.Fatalf("InstanceCreate failed: %v", err)
				}
				if created.InstanceStatus.IPFamily != family {
					t.Fatalf("created public status family = %q, want %q", created.InstanceStatus.IPFamily, family)
				}

				got, err := publicClient.InstanceGet("v2", name, instanceType)
				if err != nil {
					t.Fatalf("InstanceGet failed: %v", err)
				}
				if got.InstanceStatus.IPFamily != family {
					t.Fatalf("get public status family = %q, want %q", got.InstanceStatus.IPFamily, family)
				}

				list, err := publicClient.InstanceList()
				if err != nil {
					t.Fatalf("InstanceList failed: %v", err)
				}
				listed, ok := list[name]
				if !ok {
					t.Fatalf("InstanceList omitted %s", name)
				}
				if listed.InstanceStatus.IPFamily != family {
					t.Fatalf("list public status family = %q, want %q", listed.InstanceStatus.IPFamily, family)
				}
				capture.mu.Lock()
				createRequest := capture.creates[name]
				capture.mu.Unlock()
				if createRequest == nil || createRequest.Spec.IpFamily != family {
					var gotFamily string
					if createRequest != nil && createRequest.Spec != nil {
						gotFamily = createRequest.Spec.IpFamily
					}
					t.Fatalf("nested create request family = %q, want %q", gotFamily, family)
				}
			})
		}
	}
}

// API7 explicit-family rejection belongs to manager capability negotiation.
// The Instance Manager seam is VersionGet: it advertises API 8 so an old
// manager can retain empty-family compatibility without a second IM-side gate.
func TestVersionGetAdvertisesIPFamilyCapability(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server, err := NewServer(ctx, t.TempDir(), "127.0.0.1:8500", "127.0.0.1:8504", nil, true)
	if err != nil {
		t.Fatalf("NewServer returned an error: %v", err)
	}
	version, err := server.VersionGet(ctx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("VersionGet returned an error: %v", err)
	}
	if version.InstanceManagerAPIVersion <= 7 {
		t.Fatalf("API %d must not advertise explicit-family support to an API7 manager", version.InstanceManagerAPIVersion)
	}
	if version.InstanceManagerAPIVersion != 8 {
		t.Fatalf("InstanceManagerAPIVersion = %d, want 8 for per-instance IP-family capability", version.InstanceManagerAPIVersion)
	}
}
