FROM golang:1.25-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
COPY vendor ./vendor

COPY . .
RUN CGO_ENABLED=0 go build -mod=vendor -installsuffix cgo -o solidfire-csi ./cmd/solidfire-csi

FROM alpine:3.19

# Install utilities for iSCSI and filesystems
RUN apk add --no-cache \
    open-iscsi \
    e2fsprogs \
    xfsprogs \
    blkid \
    util-linux \
    ca-certificates \
    # Experimental: ZFS support (userspace tools only) - Host must have zfs module loaded
    zfs

COPY --from=builder /app/solidfire-csi /bin/solidfire-csi

ENTRYPOINT ["/bin/solidfire-csi"]
