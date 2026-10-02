module sample_external_resolver

go 1.26.8

require (
	compatibility_service v0.0.0
	google.golang.org/grpc v1.83.2
)

require (
	golang.org/x/net v0.59.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/text v0.42.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260706201446-f0a921348800 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace compatibility_service => ../compatibility_service
