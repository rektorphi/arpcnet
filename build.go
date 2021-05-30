package arpcnet

//go:generate protoc -I proto/ rektorphi/arpcnet/v1/rpcframe.proto --go_out=generated --go_opt=paths=source_relative --go-grpc_out=generated --go-grpc_opt=paths=source_relative
//go:generate protoc -I proto/ rektorphi/arpcnet/v1/link_service.proto --go_out=generated --go_opt=paths=source_relative --go-grpc_out=generated --go-grpc_opt=paths=source_relative
//go:generate protoc -I proto/ rektorphi/arpcnet/test/test_service.proto --go_out=generated --go_opt=paths=source_relative --go-grpc_out=generated --go-grpc_opt=paths=source_relative

var Version = "0.3.0"
