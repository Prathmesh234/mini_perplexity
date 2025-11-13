# Indexer Agent

Notes for Common Crawl ingestion and indexing pipeline.

## 1. CLI + Runner Wiring
- Added `main.py` entry point that pipes `chunk_index` output directly into `send_chunks_to_service_bus` with CLI flags for storage/service-bus overrides, chunk sizing, and topic selection.
- Introduced `run_indexer.sh` helper that cd's into the service, prefers `uv run python main.py`, and falls back to `python main.py` so ops has a single command.

## 2. Messaging Reliability Improvements
- `embedding_ingestion.py` now serializes messages with `ensure_ascii=False` to preserve multilingual characters.
- Wrapped the send loop in `tqdm` to expose per-chunk progress and inserted a `time.sleep(1)` throttle for gentler Service Bus ingestion.

## 3. Chunk Generation Feedback
- `chunk_index.py` uses `tqdm` while iterating blobs so we can see document-level progress when scanning Azure Blob Storage.
