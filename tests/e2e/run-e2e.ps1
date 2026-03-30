#!/usr/bin/env pwsh
# Powershell script to run E2E sanity check

$ErrorActionPreference = "Stop"

function Wait-For-Status {
    param($Resource, $Name, $JsonPath, $Expected, $Timeout=180)
    Write-Host "Waiting for $Resource/$Name to be $Expected..." -NoNewline
    $start = Get-Date
    while ($true) {
        $val = try { kubectl get $Resource $Name -o=jsonpath="$JsonPath" 2>$null } catch { "" }
        if ($val -eq $Expected) {
            Write-Host " OK"
            return
        }
        if ((Get-Date) - $start -gt (New-TimeSpan -Seconds $Timeout)) {
            Write-Host " FAILED (Timeout)"
            throw "Timeout waiting for $Resource/$Name"
        }
        Start-Sleep -Seconds 2
    }
}

Write-Host "=== Starting SolidFire CSI E2E Test ==="

# 1. Create PVC
Write-Host "`n[1/6] Creating PVC..."
kubectl apply -f 01-pvc.yaml
Wait-For-Status "pvc" "test-pvc" "{.status.phase}" "Bound"

# 2. Create Pod (Attach)
Write-Host "`n[2/6] Creating Pod..."
kubectl apply -f 02-pod.yaml
Wait-For-Status "pod" "test-pod" "{.status.phase}" "Running"

# 3. Write Data Check
Write-Host "`n[3/6] Verifying Data Write..."
$out = kubectl exec test-pod -- cat /mnt/data/hello.txt
if ($out -match "Hello SolidFire") {
    Write-Host "Data check OK: $out"
} else {
    throw "Data check FAILED"
}

# 4. Snapshot
Write-Host "`n[4/6] Taking Snapshot..."
kubectl apply -f 03-snapshotclass.yaml
# Ensure any previous snapshot is fully deleted first
kubectl delete -f 04-snapshot.yaml --ignore-not-found --wait=true
kubectl apply -f 04-snapshot.yaml
# Wait for snapshot ready (true)
Wait-For-Status "volumesnapshot" "test-snapshot" "{.status.readyToUse}" "true"

# 5. Restore (Clone from Snapshot)
Write-Host "`n[5/6] Restoring from Snapshot..."
# Ensure previous restore PVC is gone
kubectl delete -f 05-restore-pvc.yaml --ignore-not-found --wait=true
kubectl apply -f 05-restore-pvc.yaml
Wait-For-Status "pvc" "test-restore-pvc" "{.status.phase}" "Bound"

# 6. Expand Original PVC
Write-Host "`n[6/6] Expanding Original PVC to 2Gi. May take a few moments..."
kubectl patch pvc test-pvc -p '{"spec":{"resources":{"requests":{"storage":"2Gi"}}}}'
# Wait for status to reflect 2Gi
Wait-For-Status "pvc" "test-pvc" "{.status.capacity.storage}" "2Gi"

Write-Host "`n=== Test Complete ==="
Write-Host "Clean up manually with: kubectl delete -f ."
