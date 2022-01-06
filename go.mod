module github.com/longhorn/longhorn-instance-manager

go 1.13

require (
	github.com/RoaringBitmap/roaring v0.4.18
	github.com/golang/protobuf v1.3.3-0.20190920234318-1680a479a2cf
	github.com/google/uuid v1.3.0
	github.com/longhorn/backupstore v0.0.0-20211109055147-56ddc538b859
	github.com/longhorn/longhorn-engine v1.3.0-preview1
	github.com/pkg/errors v0.9.1
	github.com/sirupsen/logrus v1.8.1
	github.com/tinylib/msgp v1.1.1-0.20190612170807-0573788bc2a8 // indirect
	github.com/urfave/cli v1.22.1
	golang.org/x/net v0.0.0-20200202094626-16171245cfb2
	google.golang.org/grpc v1.21.0
	gopkg.in/check.v1 v1.0.0-20160105164936-4f90aeace3a2
)

replace golang.org/x/text v0.3.2 => golang.org/x/text v0.3.3
