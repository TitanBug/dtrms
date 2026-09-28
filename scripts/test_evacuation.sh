#!/bin/bash
# Demonstrates M3's failure-triggered migration: kill a site mid-run
# and watch the manager evacuate its live instances to healthy sites.
set -e
PROJ=/home/z/my-project/dtrms
WORK=/tmp/dtrms_evac_test
mkdir -p "$WORK"
rm -f "$WORK"/* 2>/dev/null || true
cd "$PROJ"

# Start manager + 3 sites in background
python3 manager.py --config config.json --results-dir "$WORK" > "$WORK/manager.log" 2>&1 &
MPID=$!
sleep 1
python3 site.py --site-id edge-A --config config.json > "$WORK/edge-A.log" 2>&1 &
EAPID=$!
python3 site.py --site-id edge-B --config config.json > "$WORK/edge-B.log" 2>&1 &
EBPID=$!
python3 site.py --site-id core-1 --config config.json > "$WORK/core-1.log" 2>&1 &
sleep 1.5

cleanup() {
    kill $MPID $EAPID $EBPID 2>/dev/null || true
    pkill -f "site.py.*--site-id" 2>/dev/null || true
    pkill -f "manager.py" 2>/dev/null || true
    sleep 0.5
}
trap cleanup EXIT

echo "=== admit 4 instances on edge-A ==="
for i in 1 2 3 4; do
    python3 -c "
import sys; sys.path.insert(0,'.')
from common import protocol
r = protocol.request('127.0.0.1', 6000, {'type':'REQUEST','req_id':$i,'service':'UPF','ingress':'edge-A','demand':{'cpu':800,'mem':400,'bw':300},'max_latency_ms':10,'holding_s':60}, timeout=5.0)
print('  S$i:', r.get('type'), 'site=', r.get('site'))
"
    sleep 0.1
done
sleep 0.5

echo ""
echo "=== before kill: live instances ==="
python3 telerm_cli.py --manager 127.0.0.1:6000 instances

echo ""
echo "=== KILLING edge-A (PID $EAPID) ==="
kill -9 $EAPID 2>/dev/null || true
# Wait for the manager's failure detector to demote edge-A to SUSPECT
# (failure_timeout_heartbeats * heartbeat_interval_s = 3s by default)
echo "  waiting 5s for failure detector to mark SUSPECT + evacuate..."
sleep 5

echo ""
echo "=== after kill + evacuate: live instances ==="
python3 telerm_cli.py --manager 127.0.0.1:6000 instances

echo ""
echo "=== after kill + evacuate: status ==="
python3 telerm_cli.py --manager 127.0.0.1:6000 status

echo ""
echo "=== shutdown ==="
python3 telerm_cli.py --manager 127.0.0.1:6000 shutdown
sleep 1
echo "(done)"
