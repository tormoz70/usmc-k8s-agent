# K8s Agent (ARCHIVED)

> **This nested module is archived.**  
> Use the **root** repository agent: `../cmd/agent` + `../internal/`.  
> See [../README.md](../README.md).

This tree implemented an alternate NUP-style contract (`resource.list`, `file.fetch` with presigned PUT, etc.). It is kept for reference only and is not used by `make build`, root `Dockerfile`, or `deploy/`.

## Historical build (do not use for new work)

```bash
cd k8s-agent
go mod tidy
go test ./...
go build -o bin/k8s-agent ./cmd/agent
```
