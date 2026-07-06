# Full local dev environment: compose infra + kind cluster + agent + test pods.
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

$ClusterName = "k8s-agent"
$InfraTimeoutSec = 60

function Require-Command([string]$Name) {
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        Write-Error "$Name is required but not found in PATH."
    }
}

function Wait-TcpPort([string]$HostName, [int]$Port, [int]$TimeoutSec) {
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        $result = Test-NetConnection -ComputerName $HostName -Port $Port -WarningAction SilentlyContinue
        if ($result.TcpTestSucceeded) {
            return
        }
        Start-Sleep -Seconds 2
    }
    Write-Error "Timeout waiting for ${HostName}:${Port}"
}

function Wait-HttpOk([string]$Url, [int]$TimeoutSec) {
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        try {
            $resp = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 5
            if ($resp.StatusCode -eq 200) {
                return
            }
        } catch {
            # retry
        }
        Start-Sleep -Seconds 2
    }
    Write-Error "Timeout waiting for $Url"
}

function Ensure-TestPod([string]$Name, [string[]]$KubectlArgs) {
    $prev = $ErrorActionPreference
    $ErrorActionPreference = "SilentlyContinue"
    & kubectl get pod $Name -n default -o name 2>$null | Out-Null
    $exists = ($LASTEXITCODE -eq 0)
    $ErrorActionPreference = $prev
    if ($exists) {
        Write-Host "Pod '$Name' already exists in default, skipping."
        return
    }
    Write-Host "Creating pod '$Name'..."
    & kubectl @KubectlArgs
    if ($LASTEXITCODE -ne 0) {
        Write-Error "Failed to create pod '$Name'"
    }
}

Write-Host "=== dev-up: full local test environment ===" -ForegroundColor Cyan

Require-Command docker
Require-Command kind
Require-Command kubectl

Write-Host "`n[1/4] Starting docker compose (Kafka, MinIO, mock-core UI)..."
& docker compose up -d --build
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "`n[2/4] Waiting for infra..."
Wait-TcpPort "localhost" 9092 $InfraTimeoutSec
Wait-TcpPort "localhost" 9000 $InfraTimeoutSec
Wait-HttpOk "http://localhost:8090/api/health" $InfraTimeoutSec
Write-Host "Infra is ready."

Write-Host "`n[3/4] Bootstrapping kind cluster and deploying agent..."
& powershell -NoProfile -ExecutionPolicy Bypass -File hack/bootstrap.ps1
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Waiting for k8s-agent rollout..."
& kubectl rollout status deployment/k8s-agent -n k8s-agent --timeout=120s
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "`n[4/4] Creating test pods in default..."
Ensure-TestPod "test-nginx" @(
    "run", "test-nginx", "--image=nginx:1.27", "--labels=app=test", "-n", "default"
)
Ensure-TestPod "test-busybox" @(
    "run", "test-busybox", "--image=busybox:1.36", "--labels=app=test",
    "--command", "--", "sh", "-c", "while true; do echo hello; sleep 5; done", "-n", "default"
)

Write-Host ""
Write-Host "=== Environment ready ===" -ForegroundColor Green
Write-Host "  mock-core UI:  http://localhost:8090"
Write-Host "  Kafka UI:      http://localhost:8088"
Write-Host "  MinIO Console: http://localhost:9001"
Write-Host ""
Write-Host "  kubectl get pods -n k8s-agent"
Write-Host "  kubectl get pods -n default -l app=test"
Write-Host ""
Write-Host "Smoke test: open mock-core UI -> k8s-api-list-deployments -> Send command"
