module github.com/scaleoutsean/solidfire-csi

go 1.25

require (
	github.com/container-storage-interface/spec v1.12.0
	github.com/dell/goiscsi v1.13.0
	github.com/scaleoutsean/solidfire-go v0.0.1
	github.com/sirupsen/logrus v1.9.4
	github.com/spf13/cobra v1.10.2
	google.golang.org/grpc v1.60.1
)

require (
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/kubernetes-csi/csi-lib-iscsi v0.0.0-20200118015005-959f12c91ca8 // indirect
	github.com/spf13/pflag v1.0.9 // indirect
	golang.org/x/net v0.38.0 // indirect
	golang.org/x/sys v0.31.0 // indirect
	golang.org/x/text v0.23.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20231002182017-d307bd883b97 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
	gopkg.in/yaml.v2 v2.2.8 // indirect
)

replace github.com/scaleoutsean/solidfire-go => ./solidfire-go
