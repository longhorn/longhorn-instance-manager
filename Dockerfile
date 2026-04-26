# syntax=docker/dockerfile:1.23.0@sha256:2780b5c3bab67f1f76c781860de469442999ed1a0d7992a5efdf2cffc0e3d769
FROM registry.suse.com/bci/golang:1.26@sha256:79ab11123495ceeeeee1155bd164772f9bfca3050763338beaeeb645643b661c AS base

ARG TARGETARCH
ARG http_proxy
ARG https_proxy
ARG SRC_BRANCH=master
ARG SRC_TAG
ARG CACHEBUST

ENV GOLANGCI_LINT_VERSION=v2.11.4

ENV ARCH=${TARGETARCH}
ENV GOFLAGS=-mod=vendor
ENV SRC_BRANCH=${SRC_BRANCH}
ENV SRC_TAG=${SRC_TAG}

# Install packages
RUN zypper -n ref && \
    zypper update -y && \
    zypper -n install cmake wget curl git less file \
    libglib-2_0-0 libkmod-devel libnl3-devel linux-glibc-devel pkg-config \
    psmisc tox qemu-tools fuse python3-devel zlib-devel zlib-devel-static \
    bash-completion rdma-core-devel libibverbs xsltproc docbook-xsl-stylesheets \
    perl-Config-General libaio-devel glibc-devel-static glibc-devel iptables libltdl7 \
    libdevmapper1_03 iproute2 jq gcc gcc-c++ automake gettext gettext-tools libtool && \
    rm -rf /var/cache/zypp/*

# Install golangci-lint
RUN curl -fsSL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh -o /tmp/install.sh \
    && chmod +x /tmp/install.sh \
    && /tmp/install.sh -b /usr/local/bin ${GOLANGCI_LINT_VERSION}

RUN git clone https://github.com/longhorn/dep-versions.git -b ${SRC_BRANCH} /usr/src/dep-versions && \
    cd /usr/src/dep-versions && \
    if [ -n "${SRC_TAG}" ] && git show-ref --tags ${SRC_TAG} > /dev/null 2>&1; then \
        echo "Checking out tag ${SRC_TAG}"; \
        git checkout tags/${SRC_TAG}; \
    fi

RUN export REPO_OVERRIDE="" && \
    export COMMIT_ID_OVERRIDE="" && \
    bash /usr/src/dep-versions/scripts/build-libqcow.sh "${REPO_OVERRIDE}" "${COMMIT_ID_OVERRIDE}" && \
    ldconfig

WORKDIR /go/src/github.com/longhorn/longhorn-instance-manager
COPY . .

FROM base AS build
RUN ./scripts/build

FROM base AS validate
RUN ./scripts/validate && touch /validate.done

FROM base AS test
RUN ./scripts/test

FROM scratch AS build-artifacts
COPY --from=build /go/src/github.com/longhorn/longhorn-instance-manager/bin/ /bin/

FROM scratch AS test-artifacts
COPY --from=test /go/src/github.com/longhorn/longhorn-instance-manager/coverage.out /coverage.out

FROM scratch AS ci-artifacts
COPY --from=build /go/src/github.com/longhorn/longhorn-instance-manager/bin/ /bin/
COPY --from=validate /validate.done /validate.done
