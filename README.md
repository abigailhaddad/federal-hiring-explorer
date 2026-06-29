# Federal Hiring Explorer

Interactive explorer for OPM/EHRI federal accessions data, 2005–present.

**Live site:** (connect to Vercel — see below)

## What it does

- Loads a 38 MB parquet of 5.5M+ federal hire records into the browser via DuckDB-WASM
- Compare any two time periods side by side (before/after inauguration, custom date ranges)
- Filter by agency, occupation series, pay plan, grade, appointment type, supervisory status, veteran indicator, accession category
- Charts: monthly hires (dual line), age, education, prior service, pay plan, grade (all grouped A vs B)
- Stats: total hires, avg age, avg prior service, no-prior-service %, veteran %, supervisory %

## Data

Source: [OPM/EHRI Federal Workforce Data](https://data.opm.gov/explore-data/data/data-downloads), accessions files,
mirrored to [impactproject/opm-ehri-data](https://huggingface.co/datasets/impactproject/opm-ehri-data) on HuggingFace.

`build_explorer_data.py` reads all accessions parquets from HuggingFace and writes `docs/data/accessions_explorer.parquet`.
A GitHub Actions job runs daily and commits the parquet if it changed.

## Deploy to Vercel

1. Import this repo at [vercel.com/new](https://vercel.com/new)
2. Framework: Other (static)
3. Output directory: `docs` (auto-detected from `vercel.json`)
4. Add `HF_TOKEN` secret in GitHub repo settings (for the rebuild workflow)

## Run locally

```bash
python build_explorer_data.py      # rebuild parquet from HuggingFace
python -m pytest tests/ -v         # validate parquet
python -m http.server 8765 --directory docs   # serve locally
```
