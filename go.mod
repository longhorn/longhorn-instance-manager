module github.com/longhorn/longhorn-instance-manager

go 1.17

require (
	github.com/RoaringBitmap/roaring v0.4.18
	github.com/golang/protobuf v1.5.2
	github.com/google/uuid v1.3.0
	github.com/longhorn/backupstore v0.0.0-20230215044750-3912081eb7c5
	github.com/longhorn/longhorn-engine v1.4.0-rc1.0.20230216083959-55ce01559281
	github.com/pkg/errors v0.9.1
	github.com/sirupsen/logrus v1.8.1
	github.com/urfave/cli v1.22.1
	golang.org/x/net v0.6.0
	google.golang.org/grpc v1.53.0
	gopkg.in/check.v1 v1.0.0-20200227125254-8fa46927fb4f
)

require (
	github.com/aws/aws-sdk-go v1.34.2 // indirect
	github.com/c9s/goprocinfo v0.0.0-20190309065803-0b2ad9ac246b // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.0-20190314233015-f79a8a8ca69d // indirect
	github.com/felixge/httpsnoop v1.0.1 // indirect
	github.com/glycerine/go-unsnap-stream v0.0.0-20181221182339-f9677308dec2 // indirect
	github.com/go-logr/logr v1.2.3 // indirect
	github.com/gofrs/flock v0.8.1 // indirect
	github.com/golang/snappy v0.0.1 // indirect
	github.com/google/fscrypt v0.3.3 // indirect
	github.com/gorilla/handlers v1.5.1 // indirect
	github.com/honestbee/jobq v1.0.2 // indirect
	github.com/jmespath/go-jmespath v0.3.0 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/longhorn/go-iscsi-helper v0.0.0-20230214085945-21fed2bd6261 // indirect
	github.com/longhorn/sparse-tools v0.0.0-20230216042534-6e4173e9def4 // indirect
	github.com/moby/sys/mountinfo v0.6.2 // indirect
	github.com/mschoch/smat v0.0.0-20160514031455-90eadee771ae // indirect
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/philhofer/fwd v1.0.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.17 // indirect
	github.com/rancher/go-fibmap v0.0.0-20160418233256-5fc9f8c1ed47 // indirect
	github.com/russross/blackfriday/v2 v2.0.1 // indirect
	github.com/shurcooL/sanitized_anchor_name v1.0.0 // indirect
	github.com/tinylib/msgp v1.1.1-0.20190612170807-0573788bc2a8 // indirect
	github.com/willf/bitset v1.1.10 // indirect
	golang.org/x/sys v0.5.0 // indirect
	golang.org/x/text v0.7.0 // indirect
	google.golang.org/genproto v0.0.0-20230110181048-76db0878b65f // indirect
	google.golang.org/protobuf v1.28.1 // indirect
	k8s.io/apimachinery v0.26.0 // indirect
	k8s.io/klog/v2 v2.80.1 // indirect
	k8s.io/mount-utils v0.26.0 // indirect
	k8s.io/utils v0.0.0-20221107191617-1a15be271d1d // indirect
)

replace golang.org/x/text v0.3.2 => golang.org/x/text v0.3.3
