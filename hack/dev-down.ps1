# Stop local dev environment (compose + agent manifests; kind cluster kept).
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

Write-Host "Stopping docker compose..."
& docker compose down

Write-Host "Removing agent manifests from kind..."
$prev = $ErrorActionPreference
$ErrorActionPreference = "SilentlyContinue"
& kubectl delete -k deploy/overlays/local --ignore-not-found
$ErrorActionPreference = $prev

Write-Host "Done. Kind cluster 'k8s-agent' is still running."
Write-Host "Full teardown: make dev-down-full"
