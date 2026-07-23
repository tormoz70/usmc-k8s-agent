# Full local dev environment: compose infra + kind cluster + agent + test data.
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

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

Write-Host "=== dev-up: full local test environment ===" -ForegroundColor Cyan

Require-Command docker
Require-Command kind
Require-Command kubectl

Write-Host "`n[1/5] Starting docker compose (Kafka, MinIO, mock-core UI)..."
& docker compose up -d --build
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "`n[2/5] Waiting for infra..."
Wait-TcpPort "localhost" 9092 $InfraTimeoutSec
Wait-TcpPort "localhost" 9010 $InfraTimeoutSec
Wait-HttpOk "http://localhost:8090/api/health" $InfraTimeoutSec
Write-Host "Ensuring Kafka topics..."
& powershell -NoProfile -ExecutionPolicy Bypass -File hack/kafka-init.ps1
Write-Host "Infra is ready."

Write-Host "`n[3/5] Bootstrapping kind cluster and deploying agent..."
& powershell -NoProfile -ExecutionPolicy Bypass -File hack/bootstrap.ps1
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Waiting for uamc-agent rollouts..."
foreach ($dep in @("ingress", "egress", "agent-service")) {
    & kubectl rollout status "deployment/$dep" -n uamc-agent --timeout=120s
    if ($LASTEXITCODE -ne 0) {
        Write-Host "`nRollout failed for deployment/$dep. Pod status:" -ForegroundColor Red
        & kubectl get pods -n uamc-agent -l "app.kubernetes.io/component=$dep"
        & kubectl describe pods -n uamc-agent -l "app.kubernetes.io/component=$dep" | Select-String -Pattern "Error|Warning|Failed|secret" -Context 0,1
        exit $LASTEXITCODE
    }
}

Write-Host "`n[4/5] Seeding test cluster data..."
& powershell -NoProfile -ExecutionPolicy Bypass -File hack/seed-test-data.ps1
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "`n[5/5] Exposing agent HTTP on localhost:8080 (for mock-core-ui REST scenarios)..."
& powershell -NoProfile -ExecutionPolicy Bypass -File hack/port-forward-agent-http.ps1
if ($LASTEXITCODE -ne 0) {
    Write-Host 'WARN: port-forward failed - Kafka scenarios still work; REST needs: powershell -File hack/port-forward-agent-http.ps1' -ForegroundColor Yellow
}

Write-Host ""
Write-Host "=== Environment ready ===" -ForegroundColor Green
Write-Host "  mock-core UI:  http://localhost:8090"
Write-Host "  Kafka UI:      http://localhost:8088"
Write-Host "  MinIO Console: http://localhost:9011"
Write-Host "  Agent HTTP:    http://localhost:8080/healthz  (port-forward)"
Write-Host ""
Write-Host "  kubectl get pods -A -l app.kubernetes.io/part-of=test-data"
Write-Host "  kubectl logs -n test-namespace-1 logger-a -f"
Write-Host ""
Write-Host 'Smoke test: http://localhost:8090 -> Scenarios -> ui-list-pods -> Run'
Write-Host 'REST smoke:  http://localhost:8090 -> Scenarios -> rest-healthz -> Run'
