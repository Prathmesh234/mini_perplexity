# Data

- raw/: unprocessed dumps
- processed/: cleaned/parsed
- indices/: built indexes

## Current progress

- Ingestion: Common Crawl WET → Azure Blob Storage
  - Module: `cc_download_script/cc_download.py`
  - Entry point: `main.py`
  - Behavior:
    - Reads the WET index `.gz`, samples a subset, streams each WET file, and uploads to Azure.
    - Creates the Azure container if it does not exist.
    - Skips files whose blobs already exist (idempotent).
    - Prints a summary: `{"downloaded": X, "skipped_existing": Y, "attempted": Z}`.

- Configuration
  - Environment variables (put in `data/.env` or export in shell):
    - `AZURE_CONN_STR` (required): Azure Storage connection string.
    - `CONTAINER_NAME` (optional): default `commoncrawl-wet`.
    - `INDEX_URL` (optional): default in `main.py` is `https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-40/wet.paths.gz`.
      - The library default in `cc_download.py` is `CC-MAIN-2025-43`; `main.py` overrides this. Set `INDEX_URL` to pick a specific crawl.
    - `TARGET_MB` (optional): total approximate MB to download. Default `5120` (5 GB).
    - `AVG_FILE_MB` (optional): assumed average size per WET file. Default `100`.
    - `SEED` (optional): integer seed for reproducible sampling. Omit for random.
  - Example `.env`:
    - `AZURE_CONN_STR="AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net"`
    - `CONTAINER_NAME="commoncrawl-wet"`
    - `INDEX_URL="https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-40/wet.paths.gz"`
    - `TARGET_MB="2048"`
    - `AVG_FILE_MB="100"`
    - `SEED="42"`

- How to run
  - From repo root:
    - `cd data`
    - Install deps (either):
      - If you use uv: `uv sync`
      - Or with pip: `pip install -e .`
    - Create/verify `.env` in `data/`
    - Run: `python main.py`
  - Programmatic usage:
    - Import `download_cc_wet_to_azure` from `cc_download_script.cc_download` and call with parameters.

- Notes
  - The sampling size is estimated via `TARGET_MB / AVG_FILE_MB` files (min 1).
  - Upload uses streaming HTTP; progress is shown via `tqdm`.
  - Safe to re-run: existing blobs are skipped.
  - Dependencies (declared in `pyproject.toml`): `azure-storage-blob`, `dotenv`, `requests`, `tqdm`.

## Next steps (planned)

- Add validation and storage layout in `raw/`, `processed/`, and `indices/`.
- Add processing jobs to parse WET into normalized text and metadata.
- Add indexing pipeline outputting to `indices/` for retriever.\n
