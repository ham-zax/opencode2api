#!/usr/bin/env bash
set -euo pipefail

unit="${OPENCODE2API_SYSTEMD_UNIT:-opencode2api.service}"
status_url="${OPENCODE2API_STATUS_URL:-http://127.0.0.1:13339/__supervisor/status}"
# Must exceed the supervisor's WORKER_READY_TIMEOUT_MS (default 120s).
wait_seconds="${OPENCODE2API_RELOAD_WAIT_SECONDS:-150}"

before="$(curl -fsS --max-time 5 "$status_url")"
before_generation="$(printf '%s' "$before" | python3 -c 'import json,sys; print((json.load(sys.stdin).get("active") or {}).get("generation",""))')"

sudo -n systemctl reload "$unit"

for _ in $(seq 1 $((wait_seconds * 2))); do
  current="$(curl -fsS --max-time 5 "$status_url" 2>/dev/null || true)"
  if [ -n "$current" ]; then
    generation="$(printf '%s' "$current" | python3 -c 'import json,sys; print((json.load(sys.stdin).get("active") or {}).get("generation",""))' 2>/dev/null || true)"
    if [ -n "$generation" ] && [ "$generation" != "$before_generation" ]; then
      printf '%s\n' "$current"
      exit 0
    fi
  fi
  sleep 0.5
done

echo "reload did not produce a new healthy runtime generation" >&2
exit 1
