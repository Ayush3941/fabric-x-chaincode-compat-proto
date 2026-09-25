module sample_external_resolver

go 1.26.5

require (
	compatibility_service v0.0.0
	google.golang.org/grpc v1.82.0
)

require (
	golang.org/x/net v0.57.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.40.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260630182238-925bb5da69e7 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

replace compatibility_service => ../compatibility_service
