#!/usr/bin/env bash
#
# validate_output.sh — data quality checks for transformed_data.csv
#
# Usage:
#   ./scripts/validate_output.sh path/to/transformed_data.csv [EXPECTED_ROWS] [HAS_HEADER]
#   ./scripts/validate_output.sh staging/transformed_data.csv 10000 false

set -uo pipefail

FILE="${1:?usage: $0 FILE [EXPECTED_ROWS] [HAS_HEADER]}"
EXPECTED_ROWS="${2:-10000}"
HAS_HEADER="${3:-false}"
EXPECTED_FIELDS=9
errors=0

pass() { echo "  [PASS] $*"; }
fail() { echo "  [FAIL] $*"; errors=$((errors + 1)); }

[ -f "$FILE" ] || { echo "File not found: $FILE"; exit 1; }
echo "Validating $FILE"

# 1. Row count (wc -l counts newline characters)
expected_lines=$EXPECTED_ROWS
[ "$HAS_HEADER" = "true" ] && expected_lines=$((EXPECTED_ROWS + 1))
lines=$(wc -l < "$FILE")
[ "$lines" -eq "$expected_lines" ] && pass "line count = $lines" \
                                    || fail "line count = $lines (expected $expected_lines)"

# 2. Every row has the same number of fields
field_counts=$(awk -F',' '{print NF}' "$FILE" | sort -u | tr '\n' ' ')
[ "$field_counts" = "$EXPECTED_FIELDS " ] && pass "all rows have $EXPECTED_FIELDS fields" \
                                          || fail "field counts found: $field_counts"

# 3. No Windows carriage returns left
cr=$(grep -c $'\r' "$FILE" || true)
[ "$cr" -eq 0 ] && pass "no carriage returns (\\r)" || fail "$cr lines contain \\r"

# 4. File ends with a newline
[ "$(tail -c 1 "$FILE" | od -An -c | tr -d ' ')" = '\n' ] && pass "file ends with newline" \
                                                           || fail "missing trailing newline"

# 5. Vehicle type (field 4) is fully uppercase in data rows
start=1; [ "$HAS_HEADER" = "true" ] && start=2
lower=$(awk -F',' -v s="$start" 'NR>=s && $4 ~ /[a-z]/' "$FILE" | wc -l)
[ "$lower" -eq 0 ] && pass "vehicle type is uppercase in all data rows" \
                   || fail "$lower rows with lowercase vehicle type"

echo
echo "Vehicle type distribution:"
awk -F',' -v s="$start" 'NR>=s {print $4}' "$FILE" | sort | uniq -c | sort -rn

echo
if [ "$errors" -eq 0 ]; then echo "All checks passed."; else echo "$errors check(s) failed."; exit 1; fi
