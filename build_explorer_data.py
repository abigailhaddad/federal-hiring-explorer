"""
Build a consolidated parquet for the OPM explorer site.
Reads all accessions files from HuggingFace, selects needed columns,
writes docs/data/accessions_explorer.parquet.

Run monthly after new data arrives:
  python build_explorer_data.py
"""

import duckdb, json, re, os
import urllib.request

KEEP_COLS = [
    "personnel_action_effective_date_yyyymm",
    "agency",
    "agency_subelement",
    "occupational_series",
    "occupational_series_code",
    "age_bracket",
    "education_level",
    "length_of_service_years",
    "accession_category",
    "pay_plan",
    "grade",
    "appointment_type",
    "supervisory_status",
    "veteran_indicator",
    "count",
]

BASE = "https://huggingface.co/datasets/impactproject/opm-ehri-data/resolve/main/"
HF_API = "https://huggingface.co/api/datasets/impactproject/opm-ehri-data"
OUT  = "docs/data/accessions_explorer.parquet"


def list_best_accessions_files():
    """Return the accessions file paths from the HuggingFace dataset, one per
    month (highest version). Reads the dataset file listing from the HF API so
    the build is self-contained — no local metadata/file_manifest.json needed.
    """
    with urllib.request.urlopen(HF_API) as resp:
        siblings = json.load(resp)["siblings"]

    pat = re.compile(r"accessions/accessions_(\d{6})_v(\d+)\.parquet$")
    best = {}
    for s in siblings:
        m = pat.match(s["rfilename"])
        if not m:
            continue
        yyyymm, ver = m.group(1), int(m.group(2))
        if yyyymm not in best or best[yyyymm][1] < ver:
            best[yyyymm] = (s["rfilename"], ver)

    return [best[k][0] for k in sorted(best)]

def main():
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET s3_region='us-east-1';")
    con.execute("SET http_retries=5;")
    con.execute("SET http_retry_wait_ms=5000;")

    keys = list_best_accessions_files()
    if not keys:
        raise SystemExit("No accessions files found on HuggingFace — aborting.")
    print(f"Found {len(keys)} accessions files")

    cols_sql = ", ".join(KEEP_COLS)
    urls = [f"'{BASE}{k}'" for k in keys]
    union = f"SELECT {cols_sql} FROM read_parquet([{','.join(urls)}], union_by_name=true)"

    os.makedirs("docs/data", exist_ok=True)
    con.execute(f"COPY ({union}) TO '{OUT}' (FORMAT PARQUET, COMPRESSION 'zstd')")

    size_mb = os.path.getsize(OUT) / 1024 / 1024
    row_count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{OUT}')").fetchone()[0]
    print(f"Written: {OUT}")
    print(f"  Size:  {size_mb:.2f} MB")
    print(f"  Rows:  {row_count:,}")

    # print a quick summary
    print("\nDistinct agencies:", con.execute(
        f"SELECT COUNT(DISTINCT agency) FROM read_parquet('{OUT}')"
    ).fetchone()[0])
    print("Distinct sub-agencies:", con.execute(
        f"SELECT COUNT(DISTINCT agency_subelement) FROM read_parquet('{OUT}')"
    ).fetchone()[0])
    print("Distinct occ series:", con.execute(
        f"SELECT COUNT(DISTINCT occupational_series_code) FROM read_parquet('{OUT}')"
    ).fetchone()[0])
    print("Date range:", con.execute(
        f"SELECT MIN(personnel_action_effective_date_yyyymm), MAX(personnel_action_effective_date_yyyymm) FROM read_parquet('{OUT}')"
    ).fetchone())

if __name__ == "__main__":
    main()
