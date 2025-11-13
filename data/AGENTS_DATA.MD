# Data Agent

Notes for Common Crawl download + preprocessing utilities.

## 1. English-Only Filtering
- Added `cc_download_script/filter.py` with `is_english_wet_file` helper that samples gzipped WET payloads and uses `langdetect` to keep only English text.

## 2. Downloader Integration
- `cc_download_script/cc_download.py` now buffers each WET file, filters via `is_english_wet_file`, and skips uploads when the payload is not English while tracking `skipped_language` in the summary.
- Uploads switched to use in-memory bytes (`io.BytesIO(payload)`) after verification.
- Added `min_english_files` parameter + `MIN_ENGLISH_FILES` env support (via `data/main.py`) to keep downloading until a desired count of English blobs is reached.

## 3. Dependencies
- `data/pyproject.toml` now includes `langdetect>=1.0.9`; remember to run `uv sync` inside `data/` before the next download job.
