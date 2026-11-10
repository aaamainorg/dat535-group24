"""
Print the newest Bronze run from _manifest.csv as a Markdown table.

Used by the workflows to write a readable summary on the run page in GitHub Actions:
    python scripts/manifest_summary.py ~/dat535-data/prod/bronze/_manifest.csv >> $GITHUB_STEP_SUMMARY
"""

import csv                                       # read the manifest
import sys                                       # command-line argument

path = sys.argv[1]                               # manifest file given by the workflow
with open(path, newline="", encoding="utf-8") as f:
    rows = list(csv.DictReader(f))               # all manifest lines

if not rows:                                     # empty manifest -> nothing to show
    print("\n_No files in the manifest yet._")
    sys.exit(0)

last_run = rows[-1]["run_id"]                    # the run that was just executed
print(f"\n### Bronze files in run `{last_run}`\n")
print("| Dataset | Period | Status | Schema | Size (MB) |")
print("|---|---|---|---|---|")
for r in rows:
    if r["run_id"] != last_run:                  # only this run
        continue
    size = f"{int(r['size_bytes']) / 1e6:.1f}" if r.get("size_bytes") else ""   # bytes -> MB
    status = r["status"].replace("|", "/")[:80]  # keep the table intact and short
    print(f"| {r['dataset']} | {r['period']} | {status} | {r.get('schema_status', '')} | {size} |")
