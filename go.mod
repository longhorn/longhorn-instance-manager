module github.com/longhorn/longhorn-instance-manager

go 1.17

require (
	github.com/RoaringBitmap/roaring v0.4.18
	github.com/golang/protobuf v1.5.2
	github.com/google/uuid v1.3.0
<<<<<<< HEAD
	github.com/longhorn/backupstore v0.0.0-20230505043341-f871cdfdf2fc
	github.com/longhorn/longhorn-engine v1.4.2-0.20230505050105-32bc047e398a
=======
	github.com/longhorn/backupstore v0.0.0-20230627040634-5b4f2d040e9d
	github.com/longhorn/go-spdk-helper v0.0.0-20230802035240-e5fe21b6067f
	github.com/longhorn/longhorn-engine v1.4.0-rc1.0.20230804172754-4d54af9e4ccf
	github.com/longhorn/longhorn-spdk-engine v0.0.0-20230802040621-1fda4c2a0100
>>>>>>> c651afc (Use version of longhorn-engine with engine identity flags and fields)
	github.com/pkg/errors v0.9.1
	github.com/sirupsen/logrus v1.9.0
	github.com/urfave/cli v1.22.5
	golang.org/x/net v0.7.0
	google.golang.org/grpc v1.21.0
	gopkg.in/check.v1 v1.0.0-20200227125254-8fa46927fb4f
)

require (
	github.com/aws/aws-sdk-go v1.34.2 // indirect
	github.com/c9s/goprocinfo v0.0.0-20190309065803-0b2ad9ac246b // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.0-20190314233015-f79a8a8ca69d // indirect
	github.com/glycerine/go-unsnap-stream v0.0.0-20181221182339-f9677308dec2 // indirect
	github.com/go-logr/logr v1.2.3 // indirect
	github.com/gofrs/flock v0.8.1 // indirect
	github.com/golang/snappy v0.0.1 // indirect
	github.com/gorilla/handlers v1.4.2 // indirect
	github.com/honestbee/jobq v1.0.2 // indirect
	github.com/jmespath/go-jmespath v0.3.0 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/longhorn/go-iscsi-helper v0.0.0-20230215054929-acb305e1031b // indirect
	github.com/longhorn/sparse-tools v0.0.0-20230408015858-c849def39d3c // indirect
	github.com/moby/sys/mountinfo v0.6.2 // indirect
	github.com/mschoch/smat v0.0.0-20160514031455-90eadee771ae // indirect
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/philhofer/fwd v1.0.0 // indirect
	github.com/rancher/go-fibmap v0.0.0-20160418233256-5fc9f8c1ed47 // indirect
	github.com/russross/blackfriday/v2 v2.0.1 // indirect
	github.com/shurcooL/sanitized_anchor_name v1.0.0 // indirect
	github.com/tinylib/msgp v1.1.1-0.20190612170807-0573788bc2a8 // indirect
	github.com/willf/bitset v1.1.10 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	go.uber.org/multierr v1.9.0 // indirect
	golang.org/x/sys v0.6.0 // indirect
	golang.org/x/text v0.7.0 // indirect
	google.golang.org/genproto v0.0.0-20180817151627-c66870c02cf8 // indirect
	google.golang.org/protobuf v1.30.0 // indirect
	k8s.io/apimachinery v0.26.3 // indirect
	k8s.io/klog/v2 v2.90.1 // indirect
	k8s.io/mount-utils v0.26.3 // indirect
	k8s.io/utils v0.0.0-20230313181309-38a27ef9d749 // indirect
)

replace golang.org/x/text v0.3.2 => golang.org/x/text v0.3.3
