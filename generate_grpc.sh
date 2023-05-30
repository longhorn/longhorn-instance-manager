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


PKG_DIR="pkg/imrpc"
TMP_DIR_BASE=".protobuild"
TMP_DIR="${TMP_DIR_BASE}/github.com/longhorn/longhorn-instance-manager/pkg/imrpc/"
mkdir -p "${TMP_DIR}"
cp -a "${PKG_DIR}"/*.proto "${TMP_DIR}"
for PROTO in common imrpc proxy disk instance; do
    mkdir -p "integration/rpc/${PROTO}"
    python3 -m grpc_tools.protoc -I "${TMP_DIR_BASE}" -I "proto/vendor/" -I "proto/vendor/protobuf/src/" --python_out=integration/rpc/${PROTO} --grpc_python_out=integration/rpc/${PROTO} "${TMP_DIR}/${PROTO}.proto"
    protoc -I ${TMP_DIR_BASE}/ -I proto/vendor/ -I proto/vendor/protobuf/src/ "${TMP_DIR}/${PROTO}.proto" --go_out=plugins=grpc:"${TMP_DIR_BASE}"
    mv "${TMP_DIR}/${PROTO}.pb.go" "${PKG_DIR}/${PROTO}.pb.go"
done

rm -rf "${TMP_DIR_BASE}"
