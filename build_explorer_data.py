"""
Build a consolidated parquet for the OPM explorer site.
Reads all accessions files from HuggingFace, selects needed columns,
writes docs/data/accessions_explorer.parquet.

Run monthly after new data arrives:
  python build_explorer_data.py

SIZE BUDGET
-----------
The site is served from Cloudflare Pages, which enforces a hard 25 MiB
per-file limit (docs/tests/test_docs_size.py enforces it in CI). The
upstream accessions files carry one row per hire with a literal count of 1,
which produced a 40.9 MB parquet. Two changes bring it to ~10.5 MiB with no
loss of information for this site:

  1. Pre-aggregate. Every query in docs/index.html is a
     SUM(count) grouped/filtered by DIM_COLS only — none looks at an
     individual row — so rolling the rows up to one row per distinct
     DIM_COLS tuple with a summed count is exactly equivalent. That alone
     collapses 5.53M rows to 3.99M.
  2. Sort before writing. Every column is low-cardinality and
     dictionary-encoded, so ordering by the widest columns first turns them
     into long runs that ZSTD collapses. This is where most of the win is
     (26.9 MB unsorted -> 11.0 MB sorted).

Row order is irrelevant to the site (all queries GROUP BY or ORDER BY
explicitly), and ORDER BY covers every DIM col — which is unique per output
row — so the byte output is deterministic and the daily rebuild workflow
only commits on a genuine data change.
"""

import duckdb, json, re, os
import urllib.request

# Dimensions the explorer filters and groups by. These become the grouping key.
DIM_COLS = [
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
]

# Columns read from the upstream HuggingFace files.
KEEP_COLS = DIM_COLS + ["count"]

# Write order: widest / highest-cardinality columns first so their dictionary
# indices form long runs. occupational_series sits directly after its code
# because it is functionally determined by it, which makes it nearly free.
SORT_COLS = [
    "agency_subelement",
    "occupational_series_code",
    "occupational_series",
    "grade",
    "pay_plan",
    "length_of_service_years",
    "education_level",
    "age_bracket",
    "appointment_type",
    "accession_category",
    "supervisory_status",
    "veteran_indicator",
    "agency",
    "personnel_action_effective_date_yyyymm",
]
assert sorted(SORT_COLS) == sorted(DIM_COLS), "SORT_COLS must cover every DIM col"

ZSTD_LEVEL = 12
ROW_GROUP_SIZE = 250_000
MAX_BYTES = 25 * 1024 * 1024  # Cloudflare Pages hard per-file limit

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

    con.execute(f"CREATE TABLE raw AS {union}")

    # Upstream 'count' is a VARCHAR. Refuse to build rather than silently drop
    # hires if a non-numeric value ever shows up.
    bad = con.execute(
        'SELECT COUNT(*) FROM raw WHERE TRY_CAST("count" AS BIGINT) IS NULL'
    ).fetchone()[0]
    if bad:
        raise SystemExit(f"{bad:,} upstream rows have a non-numeric count — aborting.")

    raw_rows, raw_hires = con.execute(
        'SELECT COUNT(*), SUM(TRY_CAST("count" AS BIGINT)) FROM raw'
    ).fetchone()

    dims_sql = ", ".join(f'"{c}"' for c in DIM_COLS)
    order_sql = ", ".join(f'"{c}"' for c in SORT_COLS)
    agg = (
        f'SELECT {dims_sql}, CAST(SUM(TRY_CAST("count" AS BIGINT)) AS INTEGER) AS "count" '
        f"FROM raw GROUP BY ALL"
    )

    os.makedirs("docs/data", exist_ok=True)
    con.execute(
        f"COPY (SELECT * FROM ({agg}) ORDER BY {order_sql}) TO '{OUT}' "
        f"(FORMAT PARQUET, COMPRESSION 'zstd', COMPRESSION_LEVEL {ZSTD_LEVEL}, "
        f"ROW_GROUP_SIZE {ROW_GROUP_SIZE})"
    )

    # The rollup must conserve total hires exactly.
    out_rows, out_hires = con.execute(
        f"SELECT COUNT(*), SUM(\"count\") FROM read_parquet('{OUT}')"
    ).fetchone()
    if out_hires != raw_hires:
        raise SystemExit(
            f"Rollup lost hires: {raw_hires:,} in -> {out_hires:,} out — aborting."
        )

    size = os.path.getsize(OUT)
    print(f"Written: {OUT}")
    print(f"  Size:  {size / 1024 / 1024:.2f} MiB ({size:,} bytes)")
    print(f"  Rows:  {out_rows:,} (rolled up from {raw_rows:,})")
    print(f"  Hires: {out_hires:,} (conserved)")

    if size > MAX_BYTES:
        raise SystemExit(
            f"{OUT} is {size:,} bytes, over the {MAX_BYTES:,}-byte "
            "Cloudflare Pages per-file limit — aborting."
        )

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
