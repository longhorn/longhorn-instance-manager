FROM registry.suse.com/bci/bci-base:15.4

ARG ARCH=amd64

RUN zypper -n addrepo --refresh https://download.opensuse.org/repositories/system:/snappy/SLE_15/system:snappy.repo && \
    zypper -n addrepo --refresh https://download.opensuse.org/repositories/network:/utilities/SLE_15_SP4/network:utilities.repo && \
    zypper -n addrepo --refresh https://download.opensuse.org/repositories/devel:libraries:c_c++/15.4/devel:libraries:c_c++.repo && \
    zypper -n addrepo --refresh https://download.opensuse.org/repositories/devel:languages:python:Factory/15.4/devel:languages:python:Factory.repo && \
    zypper -n addrepo --refresh https://download.opensuse.org/repositories/devel:languages:python:backports/SLE_15/devel:languages:python:backports.repo && \
    zypper --gpg-auto-import-keys ref

RUN zypper -n install cmake kmod curl nfs-client nfs4-acl-tools fuse git gcc \
    libibverbs librdmacm1 rdma-core-devel perl-Config-General libaio-devel sg3_utils \
    iputils telnet iperf qemu-tools wget iproute2 xsltproc docbook-xsl-stylesheets \
    e2fsprogs e2fsprogs-devel xfsprogs xfsprogs-devel cifs-utils python3 netcat-openbsd util-linux-systemd tar

# Install grpc_health_probe
RUN wget https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.3.2/grpc_health_probe-linux-${ARCH} -O /usr/local/bin/grpc_health_probe && \
    chmod +x /usr/local/bin/grpc_health_probe

COPY bin/longhorn-instance-manager /usr/local/bin/

COPY package/instance-manager /usr/local/bin/

# Build go-spdk-helper
ENV GOLANG_ARCH_amd64=amd64 GOLANG_ARCH_arm64=arm64 GOLANG_ARCH_s390x=s390x GOLANG_ARCH=GOLANG_ARCH_${ARCH} \
    GOPATH=/go PATH=/go/bin:/usr/local/go/bin:${PATH} SHELL=/bin/bash
RUN wget -O - https://storage.googleapis.com/golang/go1.20.3.linux-${!GOLANG_ARCH}.tar.gz | tar -xzf - -C /usr/local && \
    go install golang.org/x/lint/golint@latest

ENV GO_SPDK_HELPER_DIR /usr/src/go-spdk-helper
RUN git clone https://github.com/longhorn/go-spdk-helper.git ${GO_SPDK_HELPER_DIR} && \
    cd ${GO_SPDK_HELPER_DIR} && \
    go build && \
    install -m 755 go-spdk-helper /usr/local/bin/go-spdk-helper && \
    rm -rf ${GO_SPDK_HELPER_DIR}

# Build liblonghorn
RUN cd /usr/src && \
    git clone https://github.com/rancher/liblonghorn.git && \
    cd liblonghorn && \
    git checkout 53d1c063b95efc8d949b095bd4bf04637230265f && \
    make; \
    make install

# Build TGT
RUN cd /usr/src && \
    git clone https://github.com/rancher/tgt.git && \
    cd tgt && \
    git checkout 3a8bc4823b5390e046f7aa8231ed262c0365c42c && \
    make; \
    make install

# Build SPDK
ENV SPDK_DIR /usr/src/spdk
RUN if [ ${ARCH} == "amd64" ]; then \
        git clone https://github.com/longhorn/spdk.git ${SPDK_DIR} --recursive && \
        cd ${SPDK_DIR} && \
        git checkout 980f535d3876a5a6665486ad0cfaac5d584034aa && \
        git submodule update --init && \
        ./scripts/pkgdep.sh && \
        ./configure --target-arch=nehalem --disable-tests --disable-unit-tests --disable-examples && \
        make -j$(nproc) && \
        make install && \
        rm -rf ${SPDK_DIR}; \
    fi

VOLUME /usr/local/bin

# Add Tini
ENV TINI_VERSION v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-${ARCH} /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--"]

CMD ["longhorn"]
