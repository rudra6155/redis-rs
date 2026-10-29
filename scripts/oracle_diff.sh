#!/usr/bin/env bash
# Re-run the oracle diff (redis-rs vs a real redis:7) and print the verdict.
# Needs cargo and either Docker or ORACLE_ADDR=host:port (a disposable redis:7).
# Regenerates results/oracle_report.txt; exits non-zero if any row differs.
set -euo pipefail
cd "$(dirname "$0")/.."
cargo test --locked --test protocol_oracle --test error_semantics oracle
echo
sed -n '1,4p' results/oracle_report.txt
grep '^SUMMARY' results/oracle_report.txt
if grep -qE '^ *[0-9]+  FAIL' results/oracle_report.txt; then
  grep -E '^ *[0-9]+  FAIL' results/oracle_report.txt
  exit 1
fi
