# mesh [![GoDoc](https://godoc.org/github.com/weaveworks/mesh?status.svg)](https://godoc.org/github.com/weaveworks/mesh) [![Circle CI](https://circleci.com/gh/weaveworks/mesh.svg?style=svg)](https://circleci.com/gh/weaveworks/mesh)

Mesh is a tool for building distributed applications.

Mesh implements a [gossip protocol](https://en.wikipedia.org/wiki/Gossip_protocol)
that provide membership, unicast, and broadcast functionality
with [eventually-consistent semantics](https://en.wikipedia.org/wiki/Eventual_consistency).
In CAP terms, it is AP: highly-available and partition-tolerant.

Mesh works in a wide variety of network setups, including thru NAT and firewalls, and across clouds and datacenters.
It works in situations where there is only partial connectivity,
 i.e. data is transparently routed across multiple hops when there is no direct connection between peers.
It copes with partitions and partial network failure.
It can be easily bootstrapped, typically only requiring knowledge of a single existing peer in the mesh to join.
It has built-in shared-secret authentication and encryption.
It scales to on the order of 100 peers, and has no dependencies.

## Using

Mesh is currently distributed as a Go package.
See [the API documentation](https://godoc.org/github.com/weaveworks/mesh).

We plan to offer Mesh as a standalone service + an easy-to-use API.
We will support multiple deployment scenarios, including
 as a standalone binary,
 as a container,
 as an ambassador or [sidecar](http://blog.kubernetes.io/2015/06/the-distributed-system-toolkit-patterns.html) component to an existing container,
 and as an infrastructure service in popular platforms.

## Developing

Requires Go 1.5+. Ensure `GO15VENDOREXPERIMENT=1` is set.

### Building

`go build ./...`

### Testing

`go test ./...`

### Dependencies

Mesh vendors all dependencies into the vendor directory, in the style of GO15VENDOREXPERIMENT.
You may use the tool of your choice to manage vendored dependencies.
We recommend [gvt](https://github.com/filosottile/gvt), using the fetch, update, and delete subcommands.

### Workflow

Mesh follows a typical PR workflow.
All contributions should be made as pull requests that satisfy the guidelines, below.

### Guidelines

- All code must abide [Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments)
- Names should abide [What's in a name](https://talks.golang.org/2014/names.slide#1)
- Code must build on both Linux and Darwin, via plain `go build`
- Code should have appropriate test coverage, invoked via plain `go test`

In addition, several mechanical checks are enforced.
See [the lint script](/lint) for details.

Note that the existing codebase is still being refactored to abide these rules.

