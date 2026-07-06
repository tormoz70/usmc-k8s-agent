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

function Test-KindClusterReachable([string]$Name) {
    $prev = $ErrorActionPreference
    $ErrorActionPreference = "SilentlyContinue"
    & kubectl cluster-info --context "kind-$Name" 2>$null | Out-Null
    $ok = ($LASTEXITCODE -eq 0)
    $ErrorActionPreference = $prev
    return $ok
}

function New-KindCluster([string]$Name) {
    Write-Host "Creating kind cluster '$Name'..."
    & kind create cluster --name $Name --config hack/kind-config.yaml
    if ($LASTEXITCODE -ne 0) {
        Write-Error @"
kind create failed (exit $LASTEXITCODE).

Common fixes on Windows / Docker Desktop:
  1. Docker Desktop -> Settings -> Resources: RAM >= 4 GB, CPUs >= 2
  2. Restart Docker Desktop
  3. Delete broken cluster: kind delete cluster --name $Name
  4. Retry: make kind-up

If kubelet healthz timeout: see docs/local-test-contour.md (kind troubleshooting)
"@
    }
}

if (Test-KindCluster $ClusterName) {
    Write-Host "Cluster '$ClusterName' already exists, verifying..."
    if (-not (Test-KindClusterReachable $ClusterName)) {
        Write-Host "Cluster is unreachable (control-plane likely stopped). Recreating..."
        & kind delete cluster --name $ClusterName
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
        New-KindCluster $ClusterName
    } else {
        Write-Host "Cluster is reachable, skipping create."
    }
} else {
    New-KindCluster $ClusterName
}

Write-Host "Verifying cluster..."
if (-not (Test-KindClusterReachable $ClusterName)) {
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

Write-Host "Cluster ready. mock-core UI: http://localhost:8090"
