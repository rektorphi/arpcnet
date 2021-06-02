package arpcnet

//go:generate protoc -I proto/ rektorphi/arpcnet/v1/rpcframe.proto --go_out=generated --go_opt=paths=source_relative --go-grpc_out=generated --go-grpc_opt=paths=source_relative
//go:generate protoc -I proto/ rektorphi/arpcnet/v1/link_service.proto --go_out=generated --go_opt=paths=source_relative --go-grpc_out=generated --go-grpc_opt=paths=source_relative
//go:generate protoc -I proto/ rektorphi/arpcnet/test/test_service.proto --go_out=generated --go_opt=paths=source_relative --go-grpc_out=generated --go-grpc_opt=paths=source_relative

// Version is the version of the ArpcNet code.
var Version = "0.3.4"

// TODO Sync this with latest version tag on the main branch.
