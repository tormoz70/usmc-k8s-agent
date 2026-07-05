MODULE ?= github.com/usmc/usmc-k8s-agent
BIN  ?= bin/agent
IMAGE ?= k8s-agent:dev

.PHONY: all build test lint tidy docker kind-up deploy-local mock-core clean

all: build

build:
	go build -o $(BIN) ./cmd/agent

mock-core:
	go build -o bin/mock-core ./hack/mock-core

test:
	go test ./...

tidy:
	go mod tidy

docker:
	docker build -t $(IMAGE) .

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
	powershell -NoProfile -Command "& { $$ErrorActionPreference='Stop'; $$leader = kubectl get pods -n k8s-agent -l 'app=k8s-agent,k8s-agent/leader=true' -o jsonpath='{.items[0].metadata.name}'; Write-Host \"Deleting leader $$leader\"; kubectl delete pod -n k8s-agent $$leader }"
else
chaos-leader:
	bash hack/chaos-leader-failover.sh
endif

clean:
	rm -rf $(BIN)
