# Bootstrap local kind cluster and deploy k8s-agent (Windows).
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

$ClusterName = "k8s-agent"
$Image = "k8s-agent:dev"

function Require-Command([string]$Name) {
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        Write-Error "$Name is required but not found in PATH."
    }
}

function Get-KindClusterNames {
    # kind writes "No kind clusters found." to stderr when empty — not a fatal error.
    $prev = $ErrorActionPreference
    $ErrorActionPreference = "SilentlyContinue"
    $lines = & kind get clusters 2>&1 | ForEach-Object { "$_" }
    $ErrorActionPreference = $prev
    $names = @()
    foreach ($line in $lines) {
        $t = $line.Trim()
        if ($t -ne "" -and $t -notmatch "No kind clusters found") {
            $names += $t
        }
    }
    return $names
}

function Test-KindCluster([string]$Name) {
    return (Get-KindClusterNames -contains $Name)
}

Require-Command kind
Require-Command docker
Require-Command kubectl

if (Test-KindCluster $ClusterName) {
    Write-Host "Cluster '$ClusterName' already exists, skipping create."
} else {
    Write-Host "Creating kind cluster '$ClusterName'..."
    & kind create cluster --name $ClusterName --config hack/kind-config.yaml
    if ($LASTEXITCODE -ne 0) {
        Write-Error @"
kind create failed (exit $LASTEXITCODE).

Common fixes on Windows / Docker Desktop:
  1. Docker Desktop -> Settings -> Resources: RAM >= 4 GB, CPUs >= 2
  2. Restart Docker Desktop
  3. Delete broken cluster: kind delete cluster --name $ClusterName
  4. Retry: make kind-up

If kubelet healthz timeout: see docs/local-test-contour.md (kind troubleshooting)
"@
    }
}

Write-Host "Verifying cluster..."
& kubectl cluster-info --context "kind-$ClusterName"
if ($LASTEXITCODE -ne 0) {
    Write-Error "Cluster '$ClusterName' is not reachable. Run: kind delete cluster --name $ClusterName"
}

Write-Host "Building image $Image..."
& docker build -t $Image .
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Loading image into kind..."
& kind load docker-image $Image --name $ClusterName
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Applying manifests..."
& kubectl apply -k deploy/overlays/local
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Cluster ready. Start infra: docker compose up -d"
