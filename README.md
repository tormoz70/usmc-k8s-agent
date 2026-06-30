# usmc-k8s-agent

Kubernetes Kafka Agent — in-cluster Go service for executing cluster operations via Kafka.

## Documentation

- Architecture: [docs/k8s-agent-architecture-nup.md](docs/k8s-agent-architecture-nup.md)
- Implementation plan: [todo_agent.md](todo_agent.md)

## Agent (Go)

Source code: [k8s-agent/](k8s-agent/)

### Local dev (minikube + Kafka + MinIO)

```powershell
.\scripts\dev\setup.ps1
.\scripts\dev\probe-resource-list.ps1
```

See [docs/dev-minikube.md](docs/dev-minikube.md).

### Build only

```powershell
cd k8s-agent
go mod tidy
go test ./...
go build -o bin/k8s-agent.exe ./cmd/agent
```
