FROM golang:1.25 AS builder

WORKDIR /app
COPY go.mod go.sum ./
COPY vendor ./vendor

COPY . .

# Build args
ARG VERSION=dev
ARG BUILD_DATE=unknown

RUN CGO_ENABLED=0 go build -mod=vendor -installsuffix cgo \
    -ldflags "-X 'github.com/scaleoutsean/solidfire-csi/driver.DriverVersion=${VERSION}' -X 'github.com/scaleoutsean/solidfire-csi/driver.BuildDate=${BUILD_DATE}'" \
    -o solidfire-csi ./cmd/solidfire-csi

FROM alpine:3.23
# install procps so the wrapper can use pgrep
RUN apk add --no-cache ca-certificates multipath-tools util-linux e2fsprogs xfsprogs btrfs-progs procps

# Setup minimal env
RUN mkdir -p /etc/iscsi

# Install wrapper as /usr/local/bin/iscsiadm so it is found first in PATH
COPY scripts/iscsiadm_wrapper.sh /usr/local/bin/iscsiadm
RUN chmod +x /usr/local/bin/iscsiadm

COPY --from=builder /app/solidfire-csi /bin/solidfire-csi
ENTRYPOINT ["/bin/solidfire-csi"]
