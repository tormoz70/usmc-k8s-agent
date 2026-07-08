$ErrorActionPreference = "Continue"

# Cursor/IDE terminals often miss PATH updates from a fresh Go install.
$env:Path = [System.Environment]::GetEnvironmentVariable("Path", "Machine") + ";" +
    [System.Environment]::GetEnvironmentVariable("Path", "User")

function Ensure-CommandInPath($Name, [string[]]$SearchDirs) {
    if (Get-Command $Name -ErrorAction SilentlyContinue) { return }
    foreach ($dir in $SearchDirs) {
        if (-not $dir -or -not (Test-Path $dir)) { continue }
        $candidate = Join-Path $dir "$Name.exe"
        if (Test-Path $candidate) {
            $env:Path = "$dir;$env:Path"
            return
        }
    }
}

Ensure-CommandInPath "go" @(
    "K:\Pro\GO\bin",
    "$env:USERPROFILE\go\bin",
    "$env:LOCALAPPDATA\Programs\Go\bin",
    "C:\Program Files\Go\bin",
    "C:\Go\bin"
)

$Root = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
$AgentDir = Join-Path $Root "k8s-agent"
$BinDir = Join-Path $AgentDir "bin"
$FallbackManifest = Join-Path $AgentDir "deploy/dev/kafka-minio-fallback.yaml"

function Test-Command($Name) {
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command not found: $Name"
    }
}

function Write-Step($Message) {
    Write-Host "==> $Message" -ForegroundColor Cyan
}

function Invoke-Quiet([scriptblock]$Block) {
    $prev = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    & $Block
    $code = $LASTEXITCODE
    $ErrorActionPreference = $prev
    return $code
}

function Use-SystemProxy {
    if (Test-Path Env:HTTP_PROXY) { return }
    $port = 10809
    try {
        $client = New-Object System.Net.Sockets.TcpClient
        $client.Connect("127.0.0.1", $port)
        $client.Close()
        $proxy = "http://127.0.0.1:$port"
        Write-Host "Using system proxy $proxy" -ForegroundColor DarkGray
        $env:HTTP_PROXY = $proxy
        $env:HTTPS_PROXY = $proxy
        $env:NO_PROXY = "localhost,127.0.0.1,.svc,.cluster.local"
    } catch {
        # no local proxy
    }
}

function Apply-FallbackManifest {
    # Job spec.template is immutable; wait for deletion before re-applying.
    kubectl delete job minio-init-buckets -n minio --ignore-not-found --wait=true 2>$null | Out-Null
    kubectl apply -f $FallbackManifest 2>&1 | ForEach-Object { Write-Host $_ }
    if ($LASTEXITCODE -ne 0) {
        throw "kubectl apply failed for kafka/minio fallback manifest (exit $LASTEXITCODE)"
    }
}

function Install-Kafka {
    $ns = kubectl get namespace kafka --ignore-not-found -o name 2>$null
    if (-not $ns) {
        Invoke-Quiet { kubectl create namespace kafka | Out-Null } | Out-Null
    }

    if ($env:USE_HELM -eq "1") {
        $kafkaRelease = helm list -n kafka -q -f "^kafka$" 2>$null
        if ($kafkaRelease) {
            Write-Host "Kafka Helm release already installed"
            return "helm"
        }
        Write-Host "Trying Helm install kafka (bitnami)..."
        Invoke-Quiet { helm repo add bitnami https://charts.bitnami.com/bitnami 2>$null | Out-Null } | Out-Null
        Invoke-Quiet { helm repo update 2>$null | Out-Null } | Out-Null
        $helmCode = Invoke-Quiet {
            helm install kafka bitnami/kafka `
                --namespace kafka `
                --set controller.replicaCount=1 `
                --set broker.replicaCount=1 `
                --set listeners.client.protocol=PLAINTEXT `
                --set listeners.controller.protocol=PLAINTEXT `
                --set listeners.interbroker.protocol=PLAINTEXT `
                --wait --timeout 10m
        }
        if ($helmCode -eq 0) { return "helm" }
        Write-Host "Helm kafka failed - using fallback" -ForegroundColor Yellow
        Invoke-Quiet { helm uninstall kafka -n kafka 2>$null | Out-Null } | Out-Null
    } else {
        Write-Host "Using fallback Kafka manifest (Redpanda) - set USE_HELM=1 to try Bitnami Helm"
    }

    Apply-FallbackManifest
    return "fallback"
}

function Install-MinIO {
    if ($env:USE_HELM -eq "1") {
        $minioRelease = helm list -n minio -q -f "^minio$" 2>$null
        if ($minioRelease) {
            Write-Host "MinIO Helm release already installed"
            return
        }
        Write-Host "Trying Helm install minio (bitnami)..."
        $helmCode = Invoke-Quiet {
            helm install minio bitnami/minio `
                --namespace minio `
                --set auth.rootUser=minioadmin `
                --set auth.rootPassword=minioadmin `
                --set defaultBuckets="exports,logs" `
                --set service.type=ClusterIP `
                --wait --timeout 10m
        }
        if ($helmCode -eq 0) { return }
        Write-Host "Helm minio failed - fallback manifest already applied with Kafka" -ForegroundColor Yellow
    }
    if (-not (kubectl get deploy minio -n minio --ignore-not-found -o name 2>$null)) {
        Apply-FallbackManifest
    }
}

function New-KafkaTopics($Mode) {
    $topics = @("commands.in", "commands.results", "cluster.events", "commands.dlq")
    if ($Mode -eq "helm") {
        $kafkaPod = kubectl get pods -n kafka -l app.kubernetes.io/name=kafka -o jsonpath='{.items[0].metadata.name}'
        foreach ($topic in $topics) {
            kubectl exec -n kafka $kafkaPod -- bash -c "kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic $topic --partitions 3 --replication-factor 1"
            Write-Host "  topic: $topic"
        }
        return
    }

    kubectl wait --for=condition=available deployment/kafka -n kafka --timeout=300s
    foreach ($topic in $topics) {
        kubectl exec -n kafka deploy/kafka -- rpk topic create $topic -p 3 2>$null
        Write-Host "  topic: $topic"
    }
}

Use-SystemProxy

function Configure-MinikubeProxy {
    if (-not $env:HTTPS_PROXY) { return }
    Write-Host "Configuring minikube Docker proxy: $env:HTTPS_PROXY" -ForegroundColor DarkGray
    minikube ssh -- "sudo mkdir -p /etc/systemd/system/docker.service.d && echo '[Service]' | sudo tee /etc/systemd/system/docker.service.d/http-proxy.conf && echo 'Environment=\"HTTP_PROXY=$env:HTTP_PROXY\" \"HTTPS_PROXY=$env:HTTPS_PROXY\" \"NO_PROXY=$env:NO_PROXY\"' | sudo tee -a /etc/systemd/system/docker.service.d/http-proxy.conf && sudo systemctl daemon-reload && sudo systemctl restart docker" 2>$null | Out-Null
}

Write-Step "Checking prerequisites"
@("minikube", "kubectl", "helm", "docker", "go") | ForEach-Object { Test-Command $_ }

Write-Step "Starting minikube (if not running)"
$minikubeStatus = minikube status --format='{{.Host}}' 2>$null
if ($LASTEXITCODE -ne 0 -or $minikubeStatus -ne "Running") {
    minikube start --driver=docker --cpus=4 --memory=8192
    if ($LASTEXITCODE -ne 0) { throw "minikube start failed" }
}
kubectl cluster-info | Out-Null
if ($LASTEXITCODE -ne 0) { throw "kubectl cannot reach cluster - run: minikube start" }
minikube addons enable metrics-server 2>$null | Out-Null
Configure-MinikubeProxy

Write-Step "Go mod tidy, test, and build tools"
Push-Location $AgentDir
$env:GOPROXY = "https://proxy.golang.org,direct"
$goOk = $false
for ($i = 1; $i -le 5; $i++) {
    Write-Host "  go mod tidy attempt $i/5"
    go mod tidy
    if ($LASTEXITCODE -eq 0) { $goOk = $true; break }
    Start-Sleep -Seconds 3
}
if (-not $goOk) {
    Pop-Location
    throw "go mod tidy failed after 5 attempts (check network/proxy)"
}
go test ./...
if ($LASTEXITCODE -ne 0) { Pop-Location; throw "go test failed" }
New-Item -ItemType Directory -Force -Path $BinDir | Out-Null
go build -o (Join-Path $BinDir "k8s-agent.exe") ./cmd/agent
go build -o (Join-Path $BinDir "presign.exe") ./cmd/presign
go build -o (Join-Path $BinDir "kafka-probe.exe") ./cmd/kafka-probe
Pop-Location

Write-Step "Installing Kafka"
$kafkaMode = Install-Kafka

Write-Step "Waiting for Kafka"
if ($kafkaMode -eq "helm") {
    kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=kafka -n kafka --timeout=300s
} else {
    kubectl wait --for=condition=available deployment/kafka -n kafka --timeout=300s
}

Write-Step "Creating Kafka topics"
New-KafkaTopics -Mode $kafkaMode

Write-Step "Installing MinIO"
Install-MinIO
kubectl wait --for=condition=available deployment/minio -n minio --timeout=300s 2>$null
kubectl wait --for=condition=complete job/minio-init-buckets -n minio --timeout=180s 2>$null

Write-Step "Building agent Docker image inside minikube"
Push-Location $AgentDir
minikube docker-env | Invoke-Expression
docker build -t k8s-agent:dev .
if ($LASTEXITCODE -ne 0) {
    Write-Host "Minikube docker build failed - trying host Docker + minikube image load" -ForegroundColor Yellow
    minikube docker-env -u | Invoke-Expression
    docker build -t k8s-agent:dev .
    if ($LASTEXITCODE -ne 0) {
        Pop-Location
        throw "docker build failed (check Docker Hub access / proxy). Binaries are in k8s-agent/bin/"
    }
    minikube image load k8s-agent:dev
}
Pop-Location

Write-Step "Deploying k8s-agent and demo workload"
kubectl apply -f (Join-Path $AgentDir "deploy/manifests.yaml")
kubectl apply -f (Join-Path $AgentDir "deploy/dev/agent-dev.yaml")
kubectl apply -f (Join-Path $AgentDir "deploy/dev/app-workload.yaml")
kubectl rollout status deployment/k8s-agent -n k8s-agent --timeout=180s
kubectl rollout status deployment/demo -n app --timeout=120s

Write-Step "Environment ready"
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Green
Write-Host "  1. Port-forward Kafka:  kubectl port-forward -n kafka svc/kafka 9092:9092"
Write-Host "  2. Probe resource.list: .\scripts\dev\probe-resource-list.ps1"
Write-Host "  3. Probe file.fetch:    .\scripts\dev\probe-file-fetch.ps1"
Write-Host ""
Write-Host "Agent health:" -ForegroundColor Green
Write-Host "  kubectl port-forward -n k8s-agent svc/k8s-agent 8080:8080"
Write-Host "  curl http://localhost:8080/healthz"
kubectl get pods -n k8s-agent
kubectl get pods -n app
kubectl get pods -n kafka
kubectl get pods -n minio
