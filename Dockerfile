FROM golang:1.25 AS builder

WORKDIR /app
COPY go.mod go.sum ./
COPY vendor ./vendor

COPY . .
RUN CGO_ENABLED=0 go build -mod=vendor -installsuffix cgo -o solidfire-csi ./cmd/solidfire-csi

FROM ubuntu:rolling

# Install utilities for iSCSI and filesystems
RUN apt-get update && apt-get install -y \
    open-iscsi \
    e2fsprogs \
    xfsprogs \
    btrfs-progs \
    util-linux \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/solidfire-csi /bin/solidfire-csi

# Verify nsenter exists (provided by util-linux)
RUN which nsenter

# Setup iscsiadm wrapper to use Host's iscsid
COPY iscsi-wrapper.sh /usr/local/bin/iscsi-wrapper
RUN chmod +x /usr/local/bin/iscsi-wrapper && \
    if [ -f /usr/sbin/iscsiadm ]; then mv /usr/sbin/iscsiadm /usr/sbin/iscsiadm.orig; fi && \
    ln -sf /usr/local/bin/iscsi-wrapper /usr/sbin/iscsiadm && \
    ln -sf /usr/local/bin/iscsi-wrapper /sbin/iscsiadm

# verify proper setup with which
RUN which iscsiadm

# Setup iscsiadm wrapper to use Host's iscsid
COPY iscsi-wrapper.sh /usr/local/bin/iscsi-wrapper
RUN chmod +x /usr/local/bin/iscsi-wrapper && \
    mkdir -p /host-bin && \
    ln -s /usr/local/bin/iscsi-wrapper /host-bin/iscsiadm

# Prepend /host-bin to PATH so goiscsi picks up our wrapper
ENV PATH="/host-bin:${PATH}"

ENTRYPOINT ["/bin/solidfire-csi"]
