Blob-backed index builder that turns streaming embeddings from Azure Service Bus into k-means centroids and per-centroid HNSW shards stored directly in Azure Blob Storage.

## What it does

1. **Ingest** – `service_bus.py` drains up to 3k `EmbeddingChunk` messages from the ingestion topic and validates them with Pydantic.
2. **Sample** – `sampling.py` shuffles deterministically and truncates to a training subset for k-means.
3. **Train centroids** – `centroids.py` runs k-means (configurable cluster count), writes `centroids.npy`, and records routing metadata in `centroids_metadata.json`.
4. **Build shards** – `shards.py` assigns every embedding to its nearest centroid, creates an HNSW index per centroid (`index.bin`), and serializes `vectors.npy`, `ids.json`, and shard `meta.json` entirely in-memory.
5. **Upload** – `store_blob.py` streams centroids and shard artifacts straight to the configured blob container/prefix; no local files remain after upload.

`server.py` exposes `POST /create-hnsw` to run the full pipeline for manual testing; the same modules can be reused by a background worker to append shards continuously.