# Bootstrap local kind cluster and deploy k8s-agent (Windows).
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

$Image = "k8s-agent:dev"

function Require-Command([string]$Name) {
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        Write-Error "$Name is required but not found in PATH."
    }
}

Require-Command kind
Require-Command docker
Require-Command kubectl

Write-Host "Creating kind cluster (ignore error if it already exists)..."
& kind create cluster --name k8s-agent --config hack/kind-config.yaml
if ($LASTEXITCODE -ne 0) {
    Write-Host "kind create exited with $LASTEXITCODE (cluster may already exist), continuing..."
}

Write-Host "Building image $Image..."
& docker build -t $Image .
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Loading image into kind..."
& kind load docker-image $Image --name k8s-agent
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Applying manifests..."
& kubectl apply -k deploy/overlays/local
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Cluster ready. Start infra: docker compose up -d"
