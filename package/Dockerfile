FROM registry.suse.com/bci/bci-base:15.5 AS builder

ARG ARCH=amd64

RUN zypper -n addrepo --refresh https://download.opensuse.org/repositories/system:/snappy/SLE_15/system:snappy.repo && \
    zypper -n addrepo --refresh https://download.opensuse.org/repositories/network:/utilities/SLE_15/network:utilities.repo && \
    zypper -n addrepo --refresh https://download.opensuse.org/repositories/devel:libraries:c_c++/15.5/devel:libraries:c_c++.repo && \
    zypper -n addrepo --refresh https://download.opensuse.org/repositories/devel:languages:python:Factory/15.5/devel:languages:python:Factory.repo && \
    zypper -n addrepo --refresh https://download.opensuse.org/repositories/devel:languages:python:backports/SLE_15/devel:languages:python:backports.repo && \
    zypper --gpg-auto-import-keys ref

RUN zypper -n install cmake curl wget git gcc tar gzip xsltproc docbook-xsl-stylesheets python3 meson ninja python3-pip

# Build go-spdk-helper
ENV GOLANG_ARCH_amd64=amd64 GOLANG_ARCH_arm64=arm64 GOLANG_ARCH_s390x=s390x GOLANG_ARCH=GOLANG_ARCH_${ARCH} \
    GOPATH=/go PATH=/go/bin:/usr/local/go/bin:${PATH} SHELL=/bin/bash
RUN wget -O - https://storage.googleapis.com/golang/go1.21.3.linux-${!GOLANG_ARCH}.tar.gz | tar -xzf - -C /usr/local && \
    go install golang.org/x/lint/golint@latest

ENV GO_SPDK_HELPER_DIR /usr/src/go-spdk-helper
ENV GO_SPDK_HELPER_COMMIT_ID 5fe20e7f754283634c71d422778773be2d13439c
RUN git clone https://github.com/longhorn/go-spdk-helper.git ${GO_SPDK_HELPER_DIR} && \
    cd ${GO_SPDK_HELPER_DIR} && \
    git checkout ${GO_SPDK_HELPER_COMMIT_ID} && \
    go build && \
    install -m 755 go-spdk-helper /usr/local/bin/go-spdk-helper && \
    rm -rf ${GO_SPDK_HELPER_DIR}

# Build liblonghorn
ENV LIBLONGHORN_COMMIT_ID 53d1c063b95efc8d949b095bd4bf04637230265f
RUN cd /usr/src && \
    git clone https://github.com/rancher/liblonghorn.git && \
    cd liblonghorn && \
    git checkout ${LIBLONGHORN_COMMIT_ID} && \
    make; \
    make install

# Build TGT
ENV TGT_COMMIT_ID 3a8bc4823b5390e046f7aa8231ed262c0365c42c
RUN cd /usr/src && \
    git clone https://github.com/rancher/tgt.git && \
    cd tgt && \
    git checkout ${TGT_COMMIT_ID} && \
    make; \
    make install

# Build SPDK
ENV SPDK_DIR /usr/src/spdk
ENV SPDK_COMMIT_ID 880cd9cb41a47481e46743082057781e1063dd1c
RUN git clone https://github.com/longhorn/spdk.git ${SPDK_DIR} --recursive && \
    cd ${SPDK_DIR} && \
    git checkout ${SPDK_COMMIT_ID} && \
    git submodule update --init && \
    ./scripts/pkgdep.sh && \
    if [ ${ARCH} = "amd64" ]; then \
        ./configure --target-arch=nehalem --disable-tests --disable-unit-tests --disable-examples --enable-debug && \
        make -j$(nproc) && \
        make install; \
    elif [ ${ARCH} = "arm64" ]; then \
        ./configure --target-arch=native --disable-tests --disable-unit-tests --disable-examples --enable-debug && \
        DPDKBUILD_FLAGS="-Dplatform=generic" make -j$(nproc) && \
        make install; \
    else \
        echo "Unsupported architecture: ${ARCH}"; \
        exit 1; \
    fi

# Build libjson-c-devel
ENV LIBJSONC_COMMIT_ID b4c371fa0cbc4dcbaccc359ce9e957a22988fb34
RUN cd /usr/src && \
    git clone https://github.com/json-c/json-c.git && \
    cd json-c && \
    git checkout ${LIBJSONC_COMMIT_ID} && \
    mkdir .build && \
    cd .build && \
    cmake ../ && \
    make && \
    make install

# Build nvme-cli 2.7.1
ENV NVME_CLI_DIR /usr/src/nvme-cli
ENV NVME_CLI_COMMIT_ID dcdad6f5d70ffb2fa151f229db048180671eb1fe
RUN git clone https://github.com/linux-nvme/nvme-cli.git ${NVME_CLI_DIR} && \
    cd ${NVME_CLI_DIR} && \
    git checkout ${NVME_CLI_COMMIT_ID} && \
    meson setup --force-fallback-for=libnvme .build && \
    meson compile -C .build && \
    meson install -C .build

# Install grpc_health_probe
RUN wget https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.4.24/grpc_health_probe-linux-${ARCH} -O /usr/local/bin/grpc_health_probe && \
    chmod +x /usr/local/bin/grpc_health_probe

FROM registry.suse.com/bci/bci-base:15.5 AS release

ARG ARCH=amd64

RUN zypper -n addrepo --refresh https://download.opensuse.org/repositories/system:/snappy/SLE_15/system:snappy.repo && \
    zypper -n addrepo --refresh https://download.opensuse.org/repositories/network:/utilities/SLE_15/network:utilities.repo && \
    zypper -n addrepo --refresh https://download.opensuse.org/repositories/devel:libraries:c_c++/15.5/devel:libraries:c_c++.repo && \
    zypper -n addrepo --refresh https://download.opensuse.org/repositories/devel:languages:python:Factory/15.5/devel:languages:python:Factory.repo && \
    zypper -n addrepo --refresh https://download.opensuse.org/repositories/devel:languages:python:backports/SLE_15/devel:languages:python:backports.repo && \
    zypper --gpg-auto-import-keys ref

RUN zypper -n install nfs-client nfs4-acl-tools cifs-utils sg3_utils \
    iproute2 qemu-tools e2fsprogs xfsprogs util-linux-systemd python3-pyelftools libcmocka-devel device-mapper netcat kmod jq util-linux && \
    rm -rf /var/cache/zypp/*

# Install SPDK dependencies
COPY --from=builder /usr/src/spdk/scripts /usr/src/spdk/scripts
COPY --from=builder /usr/src/spdk/include /usr/src/spdk/include
RUN sh /usr/src/spdk/scripts/pkgdep.sh

# Copy pre-built binaries from builder
COPY --from=builder \
    /usr/local/bin/grpc_health_probe \
    /usr/local/bin/spdk_* \
    /usr/local/bin/go-spdk-helper \
    /usr/local/bin/

COPY --from=builder \
    /usr/local/sbin/nvme \
    /usr/local/sbin/

COPY --from=builder \
    /usr/sbin/tgt-admin \
    /usr/sbin/tgt-setup-lun \
    /usr/sbin/tgtadm \
    /usr/sbin/tgtd \
    /usr/sbin/tgtimg \
    /usr/sbin/

COPY --from=builder \
    /usr/local/lib64 \
    /usr/local/lib64

RUN ldconfig

COPY bin/longhorn-instance-manager package/instance-manager /usr/local/bin/

VOLUME /usr/local/bin

# Add Tini
ENV TINI_VERSION v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-${ARCH} /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--"]

CMD ["longhorn"]
