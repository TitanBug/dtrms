#!/bin/bash
# Live test of telerm_cli.py against a real manager + 3 sites
set -e
PROJ=/home/z/my-project/dtrms
WORK=/tmp/dtrms_cli_test
mkdir -p "$WORK"
cd "$PROJ"

# Start manager + 3 sites in background, all logging to $WORK
python3 manager.py --config config.json --results-dir "$WORK" > "$WORK/manager.log" 2>&1 &
MPID=$!
sleep 1
python3 site.py --site-id edge-A --config config.json > "$WORK/edge-A.log" 2>&1 &
python3 site.py --site-id edge-B --config config.json > "$WORK/edge-B.log" 2>&1 &
python3 site.py --site-id core-1 --config config.json > "$WORK/core-1.log" 2>&1 &
sleep 1.5

cleanup() {
    kill $MPID 2>/dev/null || true
    pkill -f "site.py.*--site-id" 2>/dev/null || true
    pkill -f "manager.py" 2>/dev/null || true
    sleep 0.5
}
trap cleanup EXIT

echo "=== status (one-shot) ==="
python3 telerm_cli.py --manager 127.0.0.1:6000 status
echo ""
echo "=== topology ==="
python3 telerm_cli.py --manager 127.0.0.1:6000 topology
echo ""
echo "=== instances (none yet) ==="
python3 telerm_cli.py --manager 127.0.0.1:6000 instances
echo ""
echo "=== admit one UPF instance via protocol ==="
python3 -c "
import sys; sys.path.insert(0,'.')
from common import protocol
r = protocol.request('127.0.0.1', 6000, {'type':'REQUEST','req_id':1,'service':'UPF','ingress':'edge-A','demand':{'cpu':1000,'mem':512,'bw':400},'max_latency_ms':10,'holding_s':30}, timeout=5.0)
print('REQUEST reply:', r)
"
sleep 0.5
echo ""
echo "=== instances (after admit) ==="
python3 telerm_cli.py --manager 127.0.0.1:6000 instances
echo ""
echo "=== migrate S00001 -> edge-B ==="
python3 telerm_cli.py --manager 127.0.0.1:6000 migrate S00001 edge-B
echo ""
echo "=== instances (after migrate) ==="
python3 telerm_cli.py --manager 127.0.0.1:6000 instances
echo ""
echo "=== status (after migrate) ==="
python3 telerm_cli.py --manager 127.0.0.1:6000 status
echo ""
echo "=== shutdown ==="
python3 telerm_cli.py --manager 127.0.0.1:6000 shutdown
sleep 1
echo "(done)"
