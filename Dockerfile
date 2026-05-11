FROM golang:1.26.2 AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build args
ARG VERSION=dev
ARG BUILD_DATE=unknown
ARG TARGETOS
ARG TARGETARCH

RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} go build -installsuffix cgo \
    -ldflags "-X 'github.com/scaleoutsean/solidfire-csi/driver.DriverVersion=${VERSION}' -X 'github.com/scaleoutsean/solidfire-csi/driver.BuildDate=${BUILD_DATE}'" \
    -o solidfire-csi ./cmd/solidfire-csi

FROM alpine:3.23
LABEL maintainer="scaleoutSean"
LABEL vendor="Community"
LABEL org.opencontainers.image.description="SolidFire CSI Driver for Kubernetes"
# install procps so the wrapper can use pgrep
RUN apk add --no-cache ca-certificates multipath-tools util-linux e2fsprogs xfsprogs btrfs-progs procps

# Setup minimal env
RUN mkdir -p /etc/iscsi

# Install wrapper as /usr/local/bin/iscsiadm so it is found first in PATH
COPY scripts/iscsiadm_wrapper.sh /usr/local/bin/iscsiadm
RUN chmod +x /usr/local/bin/iscsiadm

COPY --from=builder /app/solidfire-csi /bin/solidfire-csi
ENTRYPOINT ["/bin/solidfire-csi"]
