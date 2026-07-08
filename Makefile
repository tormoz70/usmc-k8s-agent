MODULE ?= github.com/usmc/usmc-k8s-agent
BIN  ?= bin/agent
IMAGE ?= k8s-agent:dev

.PHONY: all build test test-integration lint tidy docker kind-up deploy-local mock-core mock-core-ui dev-up dev-down dev-down-full seed-test-data clean

all: build

build:
	go build -o $(BIN) ./cmd/agent

mock-core:
	go build -o bin/mock-core ./hack/mock-core

mock-core-ui:
	go build -o bin/mock-core-ui ./hack/mock-core-ui

test:
	go test ./...

test-integration:
	RUN_INTEGRATION=1 go test -tags=integration ./hack/mockcorelib/... -timeout 20m -v

tidy:
	go mod tidy

docker:
	docker build -t $(IMAGE) .

ifeq ($(OS),Windows_NT)
dev-up:
	powershell -NoProfile -ExecutionPolicy Bypass -File hack/dev-up.ps1
dev-down:
	powershell -NoProfile -ExecutionPolicy Bypass -File hack/dev-down.ps1
else
dev-up:
	hack/dev-up.sh
dev-down:
	hack/dev-down.sh
endif

dev-down-full: dev-down
	kind delete cluster --name k8s-agent

ifeq ($(OS),Windows_NT)
seed-test-data:
	powershell -NoProfile -ExecutionPolicy Bypass -File hack/seed-test-data.ps1
else
seed-test-data:
	hack/seed-test-data.sh
endif

ifeq ($(OS),Windows_NT)
kind-up:
	powershell -NoProfile -ExecutionPolicy Bypass -File hack/bootstrap.ps1
else
kind-up:
	hack/bootstrap.sh
endif

deploy-local:
	kubectl apply -k deploy/overlays/local

deploy-prod:
	kubectl apply -k deploy/overlays/prod

ifeq ($(OS),Windows_NT)
chaos-leader:
	powershell -NoProfile -Command "& { $$ErrorActionPreference='Stop'; $$leader = kubectl get pods -n uamc-agent -l 'app.kubernetes.io/component=agent-service,k8s-agent/leader=true' -o jsonpath='{.items[0].metadata.name}'; Write-Host \"Deleting leader $$leader\"; kubectl delete pod -n uamc-agent $$leader }"
else
chaos-leader:
	bash hack/chaos-leader-failover.sh
endif

clean:
	rm -rf $(BIN)
