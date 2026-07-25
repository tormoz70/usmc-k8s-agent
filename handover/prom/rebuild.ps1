# Пересборка пакетов handover/prom из корневого deploy/

# Запуск из корня репозитория:
#   powershell -File handover/prom/rebuild.ps1

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not (Test-Path (Join-Path $root 'deploy\base'))) {
  $root = Split-Path -Parent $PSScriptRoot
}
$prom = Join-Path $root 'handover\prom'

function Copy-Tree($src, $dst) {
  if (Test-Path $dst) { Remove-Item $dst -Recurse -Force }
  New-Item -ItemType Directory -Force -Path $dst | Out-Null
  Copy-Item -Path (Join-Path $src '*') -Destination $dst -Recurse -Force
}

foreach ($agent in @('agent-1', 'agent-2')) {
  $base = Join-Path $prom $agent
  Copy-Tree (Join-Path $root 'deploy\base') (Join-Path $base 'deploy\base')
  Copy-Tree (Join-Path $root 'deploy\overlays\prod') (Join-Path $base 'deploy\overlays\prod')
  Copy-Tree (Join-Path $root 'deploy\overlays\profiles\balanced') (Join-Path $base 'deploy\overlays\profiles\balanced')
  Copy-Tree (Join-Path $root 'deploy\overlays\profiles\lean') (Join-Path $base 'deploy\overlays\profiles\lean')
  New-Item -ItemType Directory -Force -Path (Join-Path $base 'contracts\fixtures'), (Join-Path $base 'docs') | Out-Null
  Copy-Item (Join-Path $root 'contracts\fixtures\registration-request.json') (Join-Path $base 'contracts\fixtures\') -Force
  Copy-Item (Join-Path $root 'contracts\fixtures\registration-request-v2.json') (Join-Path $base 'contracts\fixtures\') -Force
  Copy-Item (Join-Path $root 'contracts\fixtures\registration-response-rejected.json') (Join-Path $base 'contracts\fixtures\') -Force
  Copy-Item (Join-Path $root 'docs\agent-v1-v2.md') (Join-Path $base 'docs\') -Force
  Copy-Item (Join-Path $root 'docs\rbac-features-capacity.md') (Join-Path $base 'docs\') -Force
  Copy-Item (Join-Path $root 'deploy\README.md') (Join-Path $base 'docs\deploy-variants.md') -Force
}

Copy-Tree (Join-Path $root 'deploy\overlays\prod-v1') (Join-Path $prom 'agent-1\deploy\overlays\prod-v1')
Copy-Tree (Join-Path $root 'deploy\overlays\prod-v2') (Join-Path $prom 'agent-2\deploy\overlays\prod-v2')
Copy-Tree (Join-Path $root 'deploy\components\logs-node') (Join-Path $prom 'agent-2\deploy\components\logs-node')

Write-Host "Rebuilt $prom\agent-1 and $prom\agent-2 (README/CHECKLIST/site-values preserved)."
Write-Host 'Verify: kubectl kustomize handover/prom/agent-1 ; kubectl kustomize handover/prom/agent-2'
