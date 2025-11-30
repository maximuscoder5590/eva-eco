#!/usr/bin/env bash
set -euo pipefail
echo "Running integration smoke tests (local docker-compose)..."
docker-compose -f docker-compose.yml up -d --build
# wait for health
for i in $(seq 1 30); do
  if curl -sS http://localhost:8080/health >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
python3 - <<'PY'
import requests,sys
body = {
  "job":"daily_summary","request_id":"ci-local-001","initiator":"ci",
  "date_from":"2025-11-28","date_to":"2025-11-28","campaign_ids":[101],"channels":["email"]
}
r = requests.post("http://localhost:8080/run", json=body, timeout=60)
print("HTTP", r.status_code)
if r.status_code != 200:
  print(r.text); sys.exit(2)
j=r.json()
if not j.get("provenance") or len(j.get("provenance"))<6:
  print("Bad provenance", j); sys.exit(3)
print("Smoke OK")
PY
docker-compose -f docker-compose.yml down --volumes --remove-orphans
