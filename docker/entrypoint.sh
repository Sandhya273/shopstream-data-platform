#!/bin/sh
# Materialise the Snowflake private key (injected from Secrets Manager) to a file for dbt / connector.
set -eu
if [ -n "${SNOWFLAKE_PRIVATE_KEY:-}" ]; then
  umask 077
  printf '%s\n' "$SNOWFLAKE_PRIVATE_KEY" > /tmp/snowflake_key.p8
  export SNOWFLAKE_PRIVATE_KEY_PATH=/tmp/snowflake_key.p8
fi
case "$1" in
  python) cd /app ;;   # DQ / reconciliation jobs run from repo root
esac
exec "$@"
