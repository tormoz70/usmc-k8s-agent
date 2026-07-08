# Create local Kafka topics in Redpanda (idempotent).
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

$running = docker compose ps redpanda --status running 2>$null
if (-not $running) {
    Write-Error "Redpanda is not running. Start infra first: docker compose up -d redpanda"
}

$topics = @(
    @("k8s.commands.request", 1),
    @("core-client.dev.responses", 1),
    @("cluster.events", 1),
    @("logs.stream", 1),
    @("cluster.health", 1),
    @("agent.lifecycle", 1)
)

foreach ($spec in $topics) {
    $name = $spec[0]
    $parts = $spec[1]
    docker compose exec -T redpanda rpk topic create $name --partitions $parts --replicas 1 2>$null | Out-Null
}

Write-Host "Kafka topics:"
docker compose exec -T redpanda rpk topic list
