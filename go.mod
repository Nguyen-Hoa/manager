module github.com/Nguyen-Hoa/manager

go 1.19

// replace github.com/Nguyen-Hoa/worker => ../worker

replace github.com/Nguyen-Hoa/scheduler => ../scheduler

replace github.com/Nguyen-Hoa/predictor => ../predictor

require (
	github.com/Nguyen-Hoa/csvlogger v0.0.0-20221028172252-e2cd60b68c98
	github.com/Nguyen-Hoa/job v0.2.0
	github.com/Nguyen-Hoa/predictor v0.0.0-20221010231521-ac8d325da0ab
	github.com/Nguyen-Hoa/profile v1.2.7
	github.com/Nguyen-Hoa/scheduler v0.1.1
	github.com/Nguyen-Hoa/worker v1.8.0
	github.com/braintree/manners v0.0.0-20160418043613-82a8879fc5fd
	github.com/gin-gonic/gin v1.8.1
)

require (
	github.com/Microsoft/go-winio v0.6.0 // indirect
	github.com/Nguyen-Hoa/wattsup v1.5.0 // indirect
	github.com/docker/distribution v2.8.1+incompatible // indirect
	github.com/docker/docker v20.10.21+incompatible // indirect
	github.com/docker/go-connections v0.4.0 // indirect
	github.com/docker/go-units v0.5.0 // indirect
	github.com/galeone/tensorflow/tensorflow/go v0.0.0-20221023090153-6b7fa0680c3e // indirect
	github.com/galeone/tfgo v0.0.0-20221023090852-d89a5c7e31e1 // indirect
	github.com/gin-contrib/sse v0.1.0 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/go-playground/locales v0.14.0 // indirect
	github.com/go-playground/universal-translator v0.18.0 // indirect
	github.com/go-playground/validator/v10 v10.11.1 // indirect
	github.com/goccy/go-json v0.9.11 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/leodido/go-urn v1.2.1 // indirect
	github.com/lufia/plan9stats v0.0.0-20220913051719-115f729f3c8c // indirect
	github.com/mattn/go-isatty v0.0.16 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/opencontainers/image-spec v1.0.2 // indirect
	github.com/pelletier/go-toml/v2 v2.0.5 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/power-devops/perfstat v0.0.0-20220216144756-c35f1ee13d7c // indirect
	github.com/shirou/gopsutil/v3 v3.22.10 // indirect
	github.com/sirupsen/logrus v1.9.0 // indirect
	github.com/tklauser/go-sysconf v0.3.10 // indirect
	github.com/tklauser/numcpus v0.5.0 // indirect
	github.com/ugorji/go/codec v1.2.7 // indirect
	github.com/yusufpapurcu/wmi v1.2.2 // indirect
	golang.org/x/crypto v0.1.0 // indirect
	golang.org/x/mod v0.6.0 // indirect
	golang.org/x/net v0.1.0 // indirect
	golang.org/x/sys v0.1.0 // indirect
	golang.org/x/text v0.4.0 // indirect
	golang.org/x/tools v0.2.0 // indirect
	google.golang.org/protobuf v1.28.1 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)
