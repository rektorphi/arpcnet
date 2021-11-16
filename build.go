package arpcnet

//go:generate protoc -I proto/ rektorphi/arpcnet/v1/rpcframe.proto --go_out=generated --go_opt=paths=source_relative --go-grpc_out=generated --go-grpc_opt=paths=source_relative
//go:generate protoc -I proto/ rektorphi/arpcnet/v1/link_service.proto --go_out=generated --go_opt=paths=source_relative --go-grpc_out=generated --go-grpc_opt=paths=source_relative
//go:generate protoc -I proto/ rektorphi/arpcnet/test/test_service.proto --go_out=generated --go_opt=paths=source_relative --go-grpc_out=generated --go-grpc_opt=paths=source_relative

var _version = "0.0.0"

// Version is the version of the ArpcNet code.
func Version() string {
	return _version
}
