module github.com/longhorn/longhorn-instance-manager

go 1.17

require (
	github.com/RoaringBitmap/roaring v0.4.18
	github.com/golang/protobuf v1.5.2
	github.com/google/uuid v1.3.0
	github.com/longhorn/backupstore v0.0.0-20230306022849-8d5e216c3b33
	github.com/longhorn/longhorn-engine v1.3.3-0.20230216042703-718990dc8a35
	github.com/pkg/errors v0.9.1
	github.com/sirupsen/logrus v1.8.1
	github.com/urfave/cli v1.22.1
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
	github.com/golang/snappy v0.0.1 // indirect
	github.com/google/fscrypt v0.3.3 // indirect
	github.com/gorilla/handlers v1.4.2 // indirect
	github.com/honestbee/jobq v1.0.2 // indirect
	github.com/jmespath/go-jmespath v0.3.0 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/longhorn/go-iscsi-helper v0.0.0-20230215045129-588aa7586e4c // indirect
	github.com/longhorn/sparse-tools v0.0.0-20220323120706-0bd9b4129826 // indirect
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
	golang.org/x/sys v0.5.0 // indirect
	golang.org/x/text v0.7.0 // indirect
	google.golang.org/genproto v0.0.0-20180817151627-c66870c02cf8 // indirect
	google.golang.org/protobuf v1.28.1 // indirect
	k8s.io/apimachinery v0.26.0 // indirect
	k8s.io/klog/v2 v2.80.1 // indirect
	k8s.io/mount-utils v0.26.0 // indirect
	k8s.io/utils v0.0.0-20221107191617-1a15be271d1d // indirect
)

replace golang.org/x/text v0.3.2 => golang.org/x/text v0.3.3

replace github.com/longhorn/backupstore v0.0.0-20230306022849-8d5e216c3b33 => github.com/derekbit/backupstore v0.0.0-20230325070325-76a9ec29de74

replace github.com/longhorn/longhorn-engine v1.3.3-0.20230216042703-718990dc8a35 => github.com/derekbit/longhorn-engine v1.3.3-0.20230326082704-a8c7a504b918
