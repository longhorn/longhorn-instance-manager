FROM registry.suse.com/bci/bci-base:15.3

ARG ARCH=amd64

RUN zypper -n addrepo --refresh https://download.opensuse.org/repositories/system:/snappy/SLE_15/system:snappy.repo && \
    zypper -n addrepo --refresh https://download.opensuse.org/repositories/network:utilities/SLE_15_SP3/network:utilities.repo && \
    zypper --gpg-auto-import-keys ref

RUN zypper -n install cmake kmod curl nfs-client nfs4-acl-tools fuse git gcc \
    libibverbs librdmacm1 rdma-core-devel perl-Config-General libaio-devel sg3_utils \
    iputils telnet iperf qemu-tools wget iproute2 xsltproc docbook-xsl-stylesheets \
    e2fsprogs e2fsprogs-devel xfsprogs xfsprogs-devel

RUN if [ ${ARCH} == "amd64" ]; then \
        zypper -n install autoconf libtool libunwind-devel gcc-c++; \
    fi

# Install grpc_health_probe
RUN wget https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/v0.3.2/grpc_health_probe-linux-${ARCH} -O /usr/local/bin/grpc_health_probe && \
    chmod +x /usr/local/bin/grpc_health_probe

COPY bin/longhorn-instance-manager /usr/local/bin/

COPY package/engine-manager /usr/local/bin/

# Build liblonghorn
RUN cd /usr/src && \
    git clone https://github.com/rancher/liblonghorn.git && \
    cd liblonghorn && \
    git checkout fe86d7f1c6780904760f2832d8055c7ca7f0d92a && \
    make; \
    make install

# Build TGT
RUN cd /usr/src && \
    git clone https://github.com/rancher/tgt.git && \
    cd tgt && \
    git checkout 64a3443ad6ab829343167e744a757d8ec044185a && \
    make; \
    make install

VOLUME /usr/local/bin

# Add Tini
ENV TINI_VERSION v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-${ARCH} /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--"]

CMD ["longhorn"]
