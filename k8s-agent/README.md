# K8s Agent

In-cluster Go agent: Kafka commands → Kubernetes API / S3, watch and log streaming events.

## Build

```bash
cd k8s-agent
go mod tidy
go build -o bin/k8s-agent ./cmd/agent
go build -o bin/presign ./cmd/presign
go build -o bin/kafka-probe ./cmd/kafka-probe
go test ./...
```

## Local dev (minikube)

Full stack: Kafka + MinIO + agent + demo workload.

```powershell
cd k:\Project\agent
.\scripts\dev\setup.ps1
.\scripts\dev\probe-resource-list.ps1
.\scripts\dev\probe-file-fetch.ps1
```

See [../docs/dev-minikube.md](../docs/dev-minikube.md) for troubleshooting.

## Run locally (without k8s deploy)

```bash
export KAFKA_BROKERS=localhost:9092
export KUBECONFIG=~/.kube/config
export LEADER_ELECTION_ENABLED=false
./bin/k8s-agent
```

## Deploy (production manifests)

```bash
kubectl apply -f deploy/manifests.yaml
```

Dev overlay (minikube):

```bash
kubectl apply -f deploy/manifests.yaml
kubectl apply -f deploy/dev/agent-dev.yaml
kubectl apply -f deploy/dev/app-workload.yaml
```

## Kafka topics

- `commands.in` — incoming commands from core
- `commands.results` — command results
- `cluster.events` — watch and log-stream events
- `commands.dlq` — poison messages

See [../todo_agent.md](../todo_agent.md) for the full implementation plan.
