# Apply demo workloads (namespaces, deployments, log generators) to the kind cluster.
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

Write-Host "Applying test data (deploy/test-data)..."
& kubectl apply -k deploy/test-data
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Waiting for deployments..."
$deployments = @(
    @{ ns = "default"; name = "web" },
    @{ ns = "default"; name = "api" },
    @{ ns = "payments"; name = "billing-api" },
    @{ ns = "catalog"; name = "products" },
    @{ ns = "catalog"; name = "indexer" },
    @{ ns = "demo"; name = "worker" }
)
foreach ($d in $deployments) {
    & kubectl rollout status "deployment/$($d.name)" -n $d.ns --timeout=90s
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

Write-Host ""
Write-Host "Test data ready:"
Write-Host "  kubectl get pods -A -l app.kubernetes.io/part-of=test-data"
Write-Host "  kubectl logs -n default logger-a -f"
Write-Host "  kubectl logs -n payments deploy/billing-api -f"
