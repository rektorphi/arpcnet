# ArpcNet [![Go](https://github.com/rektorphi/arpcnet/actions/workflows/go.yml/badge.svg)](https://github.com/rektorphi/arpcnet/actions/workflows/go.yml)[![Go Report Card](https://goreportcard.com/badge/rektorphi/arpcnet)](https://goreportcard.com/report/rektorphi/arpcnet) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/rektorphi/arpcnet/blob/main/LICENSE)
An RPC network based on hierachical groups of nodes written in Go.
The network routes and transports remote procedure calls across a network of linked nodes and can potentially support different entry and exit protocols. However, currently only gRPC is supported.

Methods provided by gRPC servers can be registers at nodes of the network under hierarchical addresss. 
This address is like a file in a directory or a variable in a package.
A gRPC client can call these methods from any other node on the network. 
The client just makes a regular gRPC call by connecting directly to an Arpc node, which functions as a gRPC gateway.
The client must only provide the full ArpcNet address in gRPC metadata and the call is routed and tunneled by the network to the destination gRPC service.
