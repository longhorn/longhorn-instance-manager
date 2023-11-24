FROM registry.suse.com/bci/bci-base:15.5 AS builder

ARG ARCH=amd64

RUN zypper -n addrepo --refresh https://download.opensuse.org/repositories/system:/snappy/SLE_15/system:snappy.repo && \
    zypper -n addrepo --refresh https://download.opensuse.org/repositories/network:/utilities/SLE_15_SP5/network:utilities.repo && \
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
RUN git clone https://github.com/longhorn/go-spdk-helper.git ${GO_SPDK_HELPER_DIR} && \
    cd ${GO_SPDK_HELPER_DIR} && \
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
ENV SPDK_COMMIT_ID 69f656824e777feea4178d7f9b0591992cbb7b69
RUN git clone https://github.com/longhorn/spdk.git ${SPDK_DIR} --recursive && \
    if [ ${ARCH} == "amd64" ]; then \
        cd ${SPDK_DIR} && \
        git checkout ${SPDK_COMMIT_ID} && \
        git submodule update --init && \
        ./scripts/pkgdep.sh && \
        ./configure --target-arch=nehalem --disable-tests --disable-unit-tests --disable-examples && \
        make -j$(nproc) && \
        make install; \
    fi

# Build nvme-cli 2.5
ENV NVME_CLI_DIR /usr/src/nvme-cli
ENV NVME_CLI_COMMIT_ID d6c07e0de9be777009ebb9ab7475bee1ae3e0e95
RUN git clone https://github.com/linux-nvme/nvme-cli.git ${NVME_CLI_DIR} && \
    cd ${NVME_CLI_DIR} && \
    git checkout ${NVME_CLI_COMMIT_ID} && \
    meson setup --force-fallback-for=libnvme .build && \
    meson compile -C .build && \
    meson install -C .build --destdir /usr/local/nvme

FROM registry.suse.com/bci/bci-base:15.4 AS release

ARG ARCH=amd64

RUN zypper -n addrepo --refresh https://download.opensuse.org/repositories/system:/snappy/SLE_15/system:snappy.repo && \
    zypper -n addrepo --refresh https://download.opensuse.org/repositories/network:/utilities/SLE_15_SP4/network:utilities.repo && \
    zypper -n addrepo --refresh https://download.opensuse.org/repositories/devel:libraries:c_c++/15.4/devel:libraries:c_c++.repo && \
    zypper -n addrepo --refresh https://download.opensuse.org/repositories/devel:languages:python:Factory/15.4/devel:languages:python:Factory.repo && \
    zypper -n addrepo --refresh https://download.opensuse.org/repositories/devel:languages:python:backports/SLE_15/devel:languages:python:backports.repo && \
    zypper --gpg-auto-import-keys ref

RUN zypper -n install kmod curl fuse wget \
    nfs-client nfs4-acl-tools cifs-utils \
    libibverbs librdmacm1 rdma-core-devel perl-Config-General libaio-devel sg3_utils \
    iputils telnet iproute2 qemu-tools \
    e2fsprogs e2fsprogs-devel xfsprogs xfsprogs-devel util-linux-systemd python3

# Install SPDK dependencies
COPY --from=builder /usr/src/spdk/scripts /spdk/
RUN if [ "${ARCH}" == "amd64" ]; then \
        sh /spdk/pkgdep.sh; \
        rm -rf /tmp/spdk; \
    fi

# Copy pre-built binaries from builder
COPY --from=builder \
    /usr/local/bin/spdk_* \
    /usr/sbin/tgt-admin \
    /usr/sbin/tgt-setup-lun \
    /usr/sbin/tgtadm \
    /usr/sbin/tgtd \
    /usr/sbin/tgtimg \
    /usr/local/bin/go-spdk-helper \
    /usr/local/nvme/usr/local/sbin/nvme \
    /usr/local/bin/

COPY --from=builder \
    /usr/local/nvme/usr/local/lib64 \
    /usr/local/lib64

RUN ldconfig

# Install grpc_health_probe
RUN wget https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.4.21/grpc_health_probe-linux-${ARCH} -O /usr/local/bin/grpc_health_probe && \
    chmod +x /usr/local/bin/grpc_health_probe

COPY bin/longhorn-instance-manager package/instance-manager /usr/local/bin/

VOLUME /usr/local/bin

# Add Tini
ENV TINI_VERSION v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-${ARCH} /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--"]

CMD ["longhorn"]
