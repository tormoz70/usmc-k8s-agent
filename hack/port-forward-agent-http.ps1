# Expose k8s-agent ingress HTTP on localhost:8080 for mock-core-ui REST scenarios.
# Prefer kube Service proxy from mock-core-ui (Resources / REST scenarios) — port-forward is optional.
# Usage:
#   powershell -File hack/port-forward-agent-http.ps1
#   powershell -File hack/port-forward-agent-http.ps1 -Namespace uamc-agent-v1
param(
    [string]$Namespace = "uamc-agent",
    [int]$LocalPort = 8080
)
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
$PidFile = Join-Path $Root "hack\.agent-http-pf.pid"
$LogFile = Join-Path $Root "hack\.agent-http-pf.log"
$Service = "svc/k8s-agent-http"

function Stop-ExistingForward {
    if (Test-Path $PidFile) {
        $oldPid = Get-Content $PidFile -ErrorAction SilentlyContinue
        if ($oldPid) {
            $proc = Get-Process -Id $oldPid -ErrorAction SilentlyContinue
            if ($proc) {
                Write-Host "Stopping previous port-forward (pid $oldPid)..."
                Stop-Process -Id $oldPid -Force -ErrorAction SilentlyContinue
            }
        }
        Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
    }
    Get-CimInstance Win32_Process -Filter "Name='kubectl.exe'" -ErrorAction SilentlyContinue |
        Where-Object { $_.CommandLine -match "port-forward.*k8s-agent-http" } |
        ForEach-Object {
            Write-Host "Stopping kubectl pid $($_.ProcessId)..."
            Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
        }
}

Stop-ExistingForward

Write-Host "Starting kubectl port-forward $Service -> localhost:${LocalPort} ..."
# Start-Process cannot redirect stdout+stderr to the same file; use cmd redirection.
$arg = "port-forward -n $Namespace $Service ${LocalPort}:8080 > `"$LogFile`" 2>&1"
$proc = Start-Process -FilePath "cmd.exe" -ArgumentList @("/c", "kubectl $arg") -PassThru -WindowStyle Hidden

# The cmd wrapper exits when kubectl exits; find the kubectl child.
Start-Sleep -Milliseconds 500
$kubectl = Get-CimInstance Win32_Process -Filter "Name='kubectl.exe'" -ErrorAction SilentlyContinue |
    Where-Object { $_.CommandLine -match "port-forward.*k8s-agent-http" } |
    Select-Object -First 1
if ($kubectl) {
    $kubectl.ProcessId | Set-Content $PidFile
    $watchPid = $kubectl.ProcessId
} else {
    $proc.Id | Set-Content $PidFile
    $watchPid = $proc.Id
}

$deadline = (Get-Date).AddSeconds(20)
while ((Get-Date) -lt $deadline) {
    try {
        $tcp = New-Object System.Net.Sockets.TcpClient
        $tcp.Connect("127.0.0.1", $LocalPort)
        $tcp.Close()
        Write-Host "Agent HTTP ready: http://localhost:${LocalPort}/healthz (pid $watchPid)" -ForegroundColor Green
        exit 0
    } catch {
        # not ready yet
    }
    $alive = Get-Process -Id $watchPid -ErrorAction SilentlyContinue
    if (-not $alive) {
        $log = ""
        if (Test-Path $LogFile) { $log = Get-Content $LogFile -Raw -ErrorAction SilentlyContinue }
        Write-Error "port-forward exited early. Log:`n$log"
    }
    Start-Sleep -Seconds 1
}

$log = ""
if (Test-Path $LogFile) { $log = Get-Content $LogFile -Raw -ErrorAction SilentlyContinue }
Write-Error "Timeout waiting for localhost:${LocalPort}. Log:`n$log"
