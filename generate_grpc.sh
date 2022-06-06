#!/bin/bash

set -e

# check and download dependency for gRPC code generate
if [ ! -e ./proto/vendor/protobuf/src/google/protobuf ]; then
    rm -rf ./proto/vendor/protobuf/src/google/protobuf
    DIR="./proto/vendor/protobuf/src/google/protobuf"
    mkdir -p $DIR
    wget https://raw.githubusercontent.com/protocolbuffers/protobuf/v3.9.0/src/google/protobuf/empty.proto -P $DIR
fi
DIR="./proto/vendor/github.com/longhorn/longhorn-engine/proto/ptypes"
if [ ! -e ${DIR} ]; then
    rm -rf ${DIR}
    mkdir -p $DIR
    wget https://raw.githubusercontent.com/longhorn/longhorn-engine/master/proto/ptypes/common.proto -P $DIR
    wget https://raw.githubusercontent.com/longhorn/longhorn-engine/master/proto/ptypes/controller.proto -P $DIR
    wget https://raw.githubusercontent.com/longhorn/longhorn-engine/master/proto/ptypes/syncagent.proto -P $DIR
fi

# instance manager
python3 -m grpc_tools.protoc -I pkg/imrpc -I proto/vendor/protobuf/src/ --python_out=integration/rpc/instance_manager --grpc_python_out=integration/rpc/instance_manager pkg/imrpc/imrpc.proto
protoc -I pkg/imrpc/ -I proto/vendor/protobuf/src/ pkg/imrpc/imrpc.proto --go_out=plugins=grpc:pkg/imrpc

# proxy
python3 -m grpc_tools.protoc -I pkg/imrpc -I proto/vendor/ --python_out=integration/rpc/proxy --grpc_python_out=integration/rpc/proxy pkg/imrpc/proxy.proto
protoc -I pkg/imrpc/ -I proto/vendor/ pkg/imrpc/proxy.proto --go_out=plugins=grpc:pkg/imrpc/
