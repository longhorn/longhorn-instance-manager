# Stage 1: build binary from go source code
FROM  registry.suse.com/bci/golang:1.26@sha256:f6f04957fbc16399c76b1e8266cc7d0f3b4a5434b824fb251d7e3ae559d6a910 AS gobuilder

ARG ARCH=amd64
ARG SRC_BRANCH=master
ARG SRC_TAG

RUN zypper -n ref && \
    zypper update -y

RUN zypper -n install jq wget && \
    rm -rf /var/cache/zypp/*

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
FROM registry.suse.com/bci/bci-base:15.7@sha256:b39dcf02d6ef54bd2cba185060d41435c75d87fff0f94ce0a2b73f2a24e5e66d AS cbuilder

ARG ARCH=amd64
ARG SRC_BRANCH=master
ARG SRC_TAG

# Install build dependencies from zypper
RUN zypper -n ref && \
    zypper update -y

# longhorn/longhorn#11254: python311's distutils ships wininst-*.exe stubs that
# cause false-positive vulnerability alarms. Remove them in this same layer so
# they're never copied into the release stage via `COPY --from=cbuilder`.
RUN zypper -n install cmake gcc gcc13 xsltproc docbook-xsl-stylesheets git python311 python311-pip fuse3-devel jq nasm && \
    rm -rf /var/cache/zypp/* && \
    find /usr/lib64 -depth -type f -name 'wininst-*.exe' -exec rm -f {} \;

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
FROM registry.suse.com/bci/bci-base:15.7@sha256:b39dcf02d6ef54bd2cba185060d41435c75d87fff0f94ce0a2b73f2a24e5e66d AS release

ARG ARCH=amd64

## Install runtime dependencies from zypper
RUN zypper -n ref && \
    zypper update -y

# longhorn/longhorn#11254: Remove wininst-*.exe (pulled in by python311-base's
# distutils) in this same layer so it never ends up in an image layer, instead
# of installing it and relying on a later layer to delete it.
RUN zypper -n install kmod jq util-linux procps awk qemu-tools e2fsprogs xfsprogs python311-base device-mapper netcat libfuse3-3 logrotate && \
    zypper -n install cifs-utils && \
    zypper -n install sg3_utils iproute2 && \
    zypper -n install util-linux-systemd nfs-client nfs4-acl-tools && \
    zypper clean -a && \
    find /usr/lib64 -depth -type f -name 'wininst-*.exe' -exec rm -f {} \;

RUN ln -sf /usr/bin/python3.11 /usr/local/bin/python && \
    ln -sf /usr/bin/python3.11 /usr/local/bin/python3

# Install SPDK dependencies
COPY --from=cbuilder /usr/src/spdk/scripts /usr/src/spdk/scripts
COPY --from=cbuilder /usr/src/spdk/include /usr/src/spdk/include
#
# pkgdep.sh (since SPDK longhorn-v26.05) sets up a Python venv with pip,
# which is only needed at build time and carries known vulnerabilities
# (including its vendored dependencies). Remove it from the runtime image.
# https://github.com/longhorn/spdk/blob/efc102055ebb8123d1215089cb3f2c4693af629b/scripts/pkgdep/helpers.sh
#
# longhorn/longhorn#14135: spdk/scripts/pkgdep/sles.sh's "minimal install" 
# also pulls in a full build toolchain and -devel headers/static-libs needed 
# only to build SPDK from source. The SPDK/TGT/liblonghorn binaries are already 
# prebuilt in the cbuilder stage, so strip all of this build tooling before it ships.
RUN for i in {1..10}; do \
        bash /usr/src/spdk/scripts/pkgdep.sh && break || sleep 1; \
    done && \
    /var/spdk/dependencies/pip/bin/python -m pip uninstall -y pip && \
    devel_pkgs=$(rpm -qa --qf '%{NAME}\n' | grep -E -- '-devel$' || true) && \
    zypper -n remove --clean-deps -y $devel_pkgs \
        gcc gcc-c++ gcc13 cpp cpp13 make cmake ninja meson \
        autoconf automake libtool help2man nasm binutils 2>/dev/null || true && \
    zypper clean -a

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

RUN ldconfig

COPY --chmod=0755 bin/longhorn-instance-manager /usr/local/bin/
COPY --chmod=0755 package/instance-manager /usr/local/bin/
COPY --chmod=0755 package/instance-manager-v2-prestop /usr/local/bin/
COPY --chmod=0755 package/instance-manager-liveness-probe /usr/local/bin/
COPY --chmod=0755 package/exec-logrotate /usr/local/bin/
COPY --chmod=0755 package/start-spdk-tgt /usr/local/bin/

# Verify the dependencies for the binaries
RUN ldd /usr/local/bin/* /usr/local/sbin/* /usr/sbin/* | grep "not found" && exit 1 || true

# Add Tini
ENV TINI_VERSION v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini-${ARCH} /tini
RUN chmod +x /tini
ENTRYPOINT ["/tini", "--"]

CMD ["longhorn"]
