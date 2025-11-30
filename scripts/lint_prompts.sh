#!/usr/bin/env bash
set -euo pipefail
echo "Linting prompts (basic checks)..."
# check for tabs (non-fatal), show filename with tabs if found
grep -R --line-number $'\t' . || true
for f in subordinate_prompt_*.md metaprompt_EVA-ECO.md protocol.md README.md; do
  if [ -f "$f" ]; then
    if ! grep -q "version:" "$f"; then
      echo "File $f missing version:" >&2
      exit 2
    fi
  fi
done
echo "Lint OK"
