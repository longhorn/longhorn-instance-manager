module github.com/longhorn/longhorn-instance-manager

go 1.17

require (
	github.com/RoaringBitmap/roaring v0.4.18
	github.com/golang/protobuf v1.3.3-0.20190920234318-1680a479a2cf
	github.com/google/uuid v1.3.0
	github.com/longhorn/backupstore v0.0.0-20221206091653-a8c2a47673e2
	github.com/longhorn/longhorn-engine v1.4.0-rc1.0.20221221061015-35357e3c051d
	github.com/pkg/errors v0.9.1
	github.com/sirupsen/logrus v1.8.1
	github.com/urfave/cli v1.22.1
	golang.org/x/net v0.0.0-20201021035429-f5854403a974
	google.golang.org/grpc v1.21.0
	gopkg.in/check.v1 v1.0.0-20160105164936-4f90aeace3a2
)

require (
	github.com/aws/aws-sdk-go v1.34.2 // indirect
	github.com/c9s/goprocinfo v0.0.0-20190309065803-0b2ad9ac246b // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.0-20190314233015-f79a8a8ca69d // indirect
	github.com/glycerine/go-unsnap-stream v0.0.0-20181221182339-f9677308dec2 // indirect
	github.com/gofrs/flock v0.8.1 // indirect
	github.com/golang/snappy v0.0.1 // indirect
	github.com/gorilla/handlers v1.4.2 // indirect
	github.com/honestbee/jobq v1.0.2 // indirect
	github.com/jmespath/go-jmespath v0.3.0 // indirect
	github.com/longhorn/go-iscsi-helper v0.0.0-20221219041640-6c94fb0d483a // indirect
	github.com/longhorn/sparse-tools v0.0.0-20221221020038-f6f1ea623675 // indirect
	github.com/mschoch/smat v0.0.0-20160514031455-90eadee771ae // indirect
	github.com/philhofer/fwd v1.0.0 // indirect
	github.com/rancher/go-fibmap v0.0.0-20160418233256-5fc9f8c1ed47 // indirect
	github.com/russross/blackfriday/v2 v2.0.1 // indirect
	github.com/shurcooL/sanitized_anchor_name v1.0.0 // indirect
	github.com/tinylib/msgp v1.1.1-0.20190612170807-0573788bc2a8 // indirect
	github.com/willf/bitset v1.1.10 // indirect
	golang.org/x/sys v0.0.0-20210630005230-0f9fa26af87c // indirect
	golang.org/x/text v0.3.3 // indirect
	google.golang.org/genproto v0.0.0-20180817151627-c66870c02cf8 // indirect
)

replace golang.org/x/text v0.3.2 => golang.org/x/text v0.3.3
