#!/usr/bin/env bash
set -e

mode="$1"

if [ "$mode" = "app" ]; then
  echo " Starting FastAPI app..."
  exec uvicorn backend.main:app --host 0.0.0.0 --port 8000

elif [ "$mode" = "agent" ]; then
  echo "  Starting Prefect agent..."
  exec prefect agent start --work-queue lightsail

else
  echo "  Unknown command '$mode', executing fallback..."
  exec "$@"
fi