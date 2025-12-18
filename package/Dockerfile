# Stage 1: build binary from go source code
FROM  registry.suse.com/bci/golang:1.25 AS gobuilder

ARG ARCH=amd64
ARG SRC_BRANCH=master
ARG SRC_TAG

RUN zypper -n ref && \
    zypper update -y

RUN zypper -n install jq wget

ENV GOLANG_ARCH_amd64=amd64 GOLANG_ARCH_arm64=arm64 GOLANG_ARCH_s390x=s390x GOLANG_ARCH=GOLANG_ARCH_${ARCH} \
    GOPATH=/go PATH=/go/bin:/usr/local/go/bin:${PATH} SHELL=/bin/bash
RUN go install golang.org/x/lint/golint@latest

# If TAG is explicitly set and exists in the repo, switch to the tag
RUN git clone https://github.com/longhorn/dep-versions.git -b ${SRC_BRANCH} /usr/src/dep-versions && \
    cd /usr/src/dep-versions && \
    if [ -n "${SRC_TAG}" ] && git show-ref --tags ${SRC_TAG} > /dev/null 2>&1; then \
        echo "Checking out tag ${SRC_TAG}"; \
        cd /usr/src/dep-versions && git checkout tags/${SRC_TAG}; \
    fi

# Build go-spdk-helper
RUN export REPO_OVERRIDE="" && \
    export COMMIT_ID_OVERRIDE="" && \
    bash /usr/src/dep-versions/scripts/build-go-spdk-helper.sh "${REPO_OVERRIDE}" "${COMMIT_ID_OVERRIDE}"

# Install grpc_health_probe
RUN export GRPC_HEALTH_PROBE_DOWNLOAD_URL=$(wget -qO- https://api.github.com/repos/grpc-ecosystem/grpc-health-probe/releases/latest | jq -r '.assets[] | select(.name | test("linux.*'"${ARCH}"'"; "i")) | .browser_download_url') && \
    wget ${GRPC_HEALTH_PROBE_DOWNLOAD_URL} -O /usr/local/bin/grpc_health_probe && \
    chmod +x /usr/local/bin/grpc_health_probe

# Stage 2: build binary from c source code
FROM registry.suse.com/bci/bci-base:15.7 AS cbuilder

ARG ARCH=amd64
ARG SRC_BRANCH=master
ARG SRC_TAG

# Install build dependencies from zypper
RUN zypper -n ref && \
    zypper update -y

RUN for i in {1..10}; do \
        # Set the slow mirrors in regions like Russia and Belarus to avoid timeouts
        zypper -n addrepo --refresh "https://download.opensuse.org/repositories/devel:/tools:/compiler/15.6/?AVOID_COUNTRY=ru,by" devel-tools-compiler  && \
        zypper --gpg-auto-import-keys ref && break || sleep 1; \
    done

RUN zypper -n install cmake gcc gcc13 xsltproc docbook-xsl-stylesheets git python311 python311-pip fuse3-devel jq nasm

RUN ln -sf /usr/bin/python3.11 /usr/local/bin/python && \
    ln -sf /usr/bin/python3.11 /usr/local/bin/python3 && \
    ln -sf /usr/bin/pip3.11 /usr/local/bin/pip3 && \
    ln -sf /usr/bin/pip3.11 /usr/local/bin/pip

# Install dependencies defined in dep-versions
RUN git clone https://github.com/longhorn/dep-versions.git -b ${SRC_BRANCH} /usr/src/dep-versions && \
    cd /usr/src/dep-versions && \
    if [ -n "${SRC_TAG}" ] && git show-ref --tags ${SRC_TAG} > /dev/null 2>&1; then \
        echo "Checking out tag ${SRC_TAG}"; \
        cd /usr/src/dep-versions && git checkout tags/${SRC_TAG}; \
    fi

# Build liblonghorn
RUN export REPO_OVERRIDE="" && \
    export COMMIT_ID_OVERRIDE="" && \
    bash /usr/src/dep-versions/scripts/build-liblonghorn.sh "${REPO_OVERRIDE}" "${COMMIT_ID_OVERRIDE}"

# Build TGT
RUN export REPO_OVERRIDE="" && \
    export COMMIT_ID_OVERRIDE="" && \
    bash /usr/src/dep-versions/scripts/build-tgt.sh "${REPO_OVERRIDE}" "${COMMIT_ID_OVERRIDE}"

# Build spdk
RUN export REPO_OVERRIDE="" && \
    export COMMIT_ID_OVERRIDE="" && \
    bash /usr/src/dep-versions/scripts/build-spdk.sh "${REPO_OVERRIDE}" "${COMMIT_ID_OVERRIDE}" "${ARCH}"

# Build libjson-c-devel
RUN export REPO_OVERRIDE="" && \
    export COMMIT_ID_OVERRIDE="" && \
    bash /usr/src/dep-versions/scripts/build-libjsonc.sh "${REPO_OVERRIDE}" "${COMMIT_ID_OVERRIDE}"

# Build nvme-cli
RUN export REPO_OVERRIDE="" && \
    export COMMIT_ID_OVERRIDE="" && \
    bash /usr/src/dep-versions/scripts/build-nvme-cli.sh "${REPO_OVERRIDE}" "${COMMIT_ID_OVERRIDE}"

# Stage 3: copy binaries to release image
FROM registry.suse.com/bci/bci-base:15.7 AS release

ARG ARCH=amd64

## Install runtime dependencies from zypper
RUN zypper -n ref && \
    zypper update -y

RUN zypper -n install kmod jq util-linux procps awk qemu-tools e2fsprogs xfsprogs python311-base device-mapper netcat fuse3-devel logrotate libcmocka-devel

RUN ln -sf /usr/bin/python3.11 /usr/local/bin/python && \
    ln -sf /usr/bin/python3.11 /usr/local/bin/python3

RUN zypper -n install cifs-utils
RUN zypper -n install sg3_utils iproute2
RUN zypper -n install util-linux-systemd nfs-client nfs4-acl-tools
RUN zypper clean -a

# Install SPDK dependencies
COPY --from=cbuilder /usr/src/spdk/scripts /usr/src/spdk/scripts
COPY --from=cbuilder /usr/src/spdk/include /usr/src/spdk/include
RUN for i in {1..10}; do \
        bash /usr/src/spdk/scripts/pkgdep.sh && break || sleep 1; \
    done

# Copy pre-built binaries from cbuilder and gobuilder
COPY --from=gobuilder \
    /usr/local/bin/grpc_health_probe \
    /usr/local/bin/go-spdk-helper \
    /usr/local/bin/

COPY --from=cbuilder \
    /usr/local/bin/spdk_* \
    /usr/local/bin/

COPY --from=cbuilder \
    /usr/local/sbin/nvme \
    /usr/local/sbin/

COPY --from=cbuilder \
    /usr/sbin/tgt-admin \
    /usr/sbin/tgt-setup-lun \
    /usr/sbin/tgtadm \
    /usr/sbin/tgtd \
    /usr/sbin/tgtimg \
    /usr/sbin/

COPY --from=cbuilder \
   /usr/local/lib64 \
   /usr/local/lib64

COPY --from=cbuilder \
   /usr/lib64 \
   /usr/lib64

# longhorn/longhorn#11254: Clean up unnecessary *.exe that leads to a false alarm in vulnerability scanning
RUN find /usr/lib64 -depth -type f -name 'wininst-*.exe' -exec rm -f {} \;

RUN ldconfig

COPY bin/longhorn-instance-manager /usr/local/bin/
COPY package/instance-manager /usr/local/bin/
COPY package/instance-manager-v2-prestop /usr/local/bin/
COPY package/exec-logrotate /usr/local/bin/

# Verify the dependencies for the binaries
RUN ldd /usr/local/bin/* /usr/local/sbin/* /usr/sbin/* | grep "not found" && exit 1 || true

# Add Tini
ENV TINI_VERSION v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-${ARCH} /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--"]

CMD ["longhorn"]
