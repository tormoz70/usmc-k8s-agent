$ErrorActionPreference = "Stop"

$Root = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
$AgentDir = Join-Path $Root "k8s-agent"
$BinDir = Join-Path $AgentDir "bin"
$Presign = Join-Path $BinDir "presign.exe"
$KafkaProbe = Join-Path $BinDir "kafka-probe.exe"

if (-not (Test-Path $Presign)) {
    throw "presign not built. Run scripts/dev/setup.ps1 first."
}

$ObjectKey = "dev-export-$(Get-Date -Format 'yyyyMMdd-HHmmss').tar.gz"
$CommandId = [guid]::NewGuid().ToString()

Write-Host "Generating presigned PUT URL for $ObjectKey (in-cluster endpoint)..." -ForegroundColor Cyan
$presignedUrl = & $Presign `
    -endpoint "minio.minio.svc.cluster.local:9000" `
    -access-key "minioadmin" `
    -secret-key "minioadmin" `
    -bucket "exports" `
    -object $ObjectKey `
    -expiry 1h
if ($LASTEXITCODE -ne 0) { throw "presign failed" }
$presignedUrl = $presignedUrl.Trim()

Write-Host "Starting Kafka port-forward on localhost:9092 (background job)..." -ForegroundColor Cyan
$kafkaJob = Get-Job -Name "kafka-pf" -ErrorAction SilentlyContinue
if ($kafkaJob) {
    Stop-Job -Job $kafkaJob -Force -ErrorAction SilentlyContinue
    Remove-Job -Job $kafkaJob -Force -ErrorAction SilentlyContinue
}
$kafkaPf = Start-Job -Name "kafka-pf" -ScriptBlock {
    kubectl port-forward -n kafka svc/kafka 9092:9092
}
Start-Sleep -Seconds 3

try {
    $payload = @{
        command_id      = $CommandId
        idempotency_key = "dev:export:$CommandId"
        type            = "file.fetch"
        issued_by       = "core-prod"
        ts              = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
        dry_run         = $false
        target          = @{ group = ""; version = "v1"; kind = "Pod"; namespace = "app" }
        payload         = @{
            source          = "resource_export"
            source_params   = @{
                gvk        = @{ group = ""; version = "v1"; kind = "Pod" }
                namespaces = @("app")
            }
            destination     = @{
                presigned_put_url = $presignedUrl
                content_type      = "application/gzip"
                object_key        = $ObjectKey
                s3_uri            = "s3://exports/$ObjectKey"
            }
            local_processing = @("tar", "gzip")
        }
    } | ConvertTo-Json -Depth 10 -Compress

    $tmpFile = Join-Path $env:TEMP "file-fetch-$CommandId.json"
    Set-Content -Path $tmpFile -Value $payload -Encoding UTF8

    Write-Host "Sending file.fetch (resource_export) to commands.in..." -ForegroundColor Cyan
    & $KafkaProbe -brokers "localhost:9092" -action send -topic "commands.in" -file $tmpFile -key $CommandId
    if ($LASTEXITCODE -ne 0) { throw "send failed" }

    Write-Host "Waiting for result on commands.results (120s)..." -ForegroundColor Cyan
    & $KafkaProbe -brokers "localhost:9092" -action consume -topic "commands.results" -timeout 120s -from-beginning

    Remove-Item -Force $tmpFile -ErrorAction SilentlyContinue
} finally {
    Write-Host "Stopping Kafka port-forward job..." -ForegroundColor DarkGray
    Stop-Job -Job $kafkaPf -Force -ErrorAction SilentlyContinue
    Remove-Job -Job $kafkaPf -Force -ErrorAction SilentlyContinue
}
