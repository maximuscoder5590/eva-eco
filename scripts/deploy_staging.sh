#!/usr/bin/env bash
set -euo pipefail
echo "Building images (local) for staging push (edit DOCKERHUB_USER/DOCKERHUB_PASS env vars before run)..."
docker build -t ${DOCKERHUB_USER:-you}/eva-eco-orchestrator:staging ./orchestrator
docker build -t ${DOCKERHUB_USER:-you}/eva-eco-mocks:staging ./mocks
echo "Push images (requires DOCKERHUB creds in env):"
docker login -u "$DOCKERHUB_USER" -p "$DOCKERHUB_PASS"
docker push ${DOCKERHUB_USER:-you}/eva-eco-orchestrator:staging
docker push ${DOCKERHUB_USER:-you}/eva-eco-mocks:staging
echo "Done. Next: deploy container to your staging host (K8s/Cloud Run/etc)."
