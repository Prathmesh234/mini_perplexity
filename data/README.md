# FineWeb ingestion

This package now streams the [Hugging Face FineWeb](https://huggingface.co/datasets/HuggingFaceFW/fineweb)
dataset directly into Azure Blob Storage as gzipped JSONL shards.

## Environment variables

Set the following values (via `.env` in this directory or your shell) before
running `uv run main.py`:

- `AZURE_CONN_STR` (or `AZURE_STORAGE_CONNECTION_STRING`): Azure Blob Storage connection string.
- `CONTAINER_NAME`: Target container (default: `fineweb-raw`).
- `DATASET_ID`: Hugging Face dataset id (default: `HuggingFaceFW/fineweb`).
- `DATASET_SPLIT`: Dataset split to stream (default: `train`).
- `CHUNK_SIZE`: Number of records per gzipped blob (default: `10_000`).
- `BLOB_PREFIX`: Prefix/path inside the container (default: `fineweb/<split>`).
- `MAX_CHUNKS`: Optional limit on number of shards to upload.
- `UPLOAD_RETRIES`: Number of times to retry a failed blob upload (default: `3`).

## Example

```
uv run main.py
```

This loads the dataset lazily, writes each `CHUNK_SIZE` batch to a gzipped
JSONL blob under the specified prefix, and prints a summary dict when
complete.
