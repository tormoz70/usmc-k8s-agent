$ErrorActionPreference = "Stop"

$Root = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
$AgentDir = Join-Path $Root "k8s-agent"
$BinDir = Join-Path $AgentDir "bin"
$Payload = Join-Path $PSScriptRoot "payloads\resource-list.json"
$KafkaProbe = Join-Path $BinDir "kafka-probe.exe"

if (-not (Test-Path $KafkaProbe)) {
    throw "kafka-probe not built. Run scripts/dev/setup.ps1 first."
}

Write-Host "Starting Kafka port-forward on localhost:9092 (background job)..." -ForegroundColor Cyan
$existingJob = Get-Job -Name "kafka-pf" -ErrorAction SilentlyContinue
if ($existingJob) {
    Stop-Job -Job $existingJob -Force -ErrorAction SilentlyContinue
    Remove-Job -Job $existingJob -Force -ErrorAction SilentlyContinue
}
$pfJob = Start-Job -Name "kafka-pf" -ScriptBlock {
    kubectl port-forward -n kafka svc/kafka 9092:9092
}
Start-Sleep -Seconds 3

try {
    Write-Host "Sending resource.list command to commands.in..." -ForegroundColor Cyan
    & $KafkaProbe -brokers "localhost:9092" -action send -topic "commands.in" -file $Payload -key "app/pods"
    if ($LASTEXITCODE -ne 0) { throw "send failed" }

    Write-Host "Waiting for result on commands.results (30s)..." -ForegroundColor Cyan
    $env:KAFKA_BROKERS = "localhost:9092"
    & $KafkaProbe -brokers "localhost:9092" -action consume -topic "commands.results" -timeout 30s -from-beginning
} finally {
    Write-Host "Stopping Kafka port-forward job..." -ForegroundColor DarkGray
    Stop-Job -Job $pfJob -Force -ErrorAction SilentlyContinue
    Remove-Job -Job $pfJob -Force -ErrorAction SilentlyContinue
}
