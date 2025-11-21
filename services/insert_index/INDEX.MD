# Insert Index Service Snapshot

This package turns Service Bus ingestion batches into k-means centroids that we will later shard across Azure Blob Storage. Everything relevant to the centroid workflow is captured below.

## Modules

- `schema.py` – `EmbeddingChunk` (chunk text/ids/length + embedding) used to validate every message.
- `service_bus.py`
  - `load_env_config()` loads `.env` (connection string + topic names).
  - `receive_all_embeddings()` drains batches of up to 3k messages, validates them as `EmbeddingChunk`, and returns the objects for downstream sampling.
  - `_deserialize_message_body()`/`publish_chunk()` handle JSON parsing + optional re-publish hooks.
- `sampling.py` – `sample_embeddings()` shuffles deterministically, truncates to 3k, and returns a float32 NumPy matrix.
- `centroids.py` – `create_centroids(max_messages, num_centroids=30)` pulls embeddings (subscription pulled from `.env`), samples, trains k-means (`random_state=42`), writes `centroids.npy`, and emits lightweight routing metadata (`centroids_metadata.json`).
- `shards.py`
  - `create_shards(chunks, centroids, ...)` fan-outs embeddings to their nearest centroid, builds the HNSW graph, and returns per-shard metadata plus raw `index.bin` / `vectors.npy` / `ids.json` bytes (now storing chunk text + ids) ready for blob uploads. `ids.json` keeps the HNSW label → chunk mapping so inference can map ANN hits back to `chunk_id`/`doc_id`/text.
- `store_blob.py`
  - `load_blob_config()` / `get_container_client()` pull `AZURE_STORAGE_CONNECTION_STRING` + `AZURE_VECTOR_CONTAINER` from `.env` and hand back an Azure Blob container client.
  - `bootstrap_vector_index_storage()` ensures `vector-indexes-client1/centroids.npy`, `metadata.json`, and a placeholder `shards/` prefix exist in Azure.
  - `upload_shard_artifacts()` streams each shard's `index.bin`, `vectors.npy`, `ids.json`, and `meta.json` bytes directly into blob storage (no local filesystem writes).
- `server.py`
  - `POST /create-hnsw`: end-to-end orchestrator that fetches embeddings, trains centroids, calls `create_shards()`, and pushes the resulting centroids + shard blobs into Azure Storage (returns uploaded blob paths). Breaks early with `"status": "empty"` when Service Bus has no data.
  - `GET /health`: simple readiness probe.
- `main.py` – still a stub; replace with the blob-backed insertion worker later.

## TL;DR Status

- ✅ HNSW shards now upload directly to Azure Blob Storage (`/create-hnsw` stitches together centroid creation, shard building, and blob writes).
- ✅ Storage tooling (`store_blob.py`) bootstraps the `vector-indexes-client1/` prefix + shards in one shot, no local files needed.
- ✅ Azure validation script (temporary) confirmed 30 shard blobs (`index.bin`, `vectors.npy`, `ids.json`, `meta.json`) exist for the latest run.
- ⚙️ Remaining: swap `/create-hnsw` from temporary API to a long-running worker, add logging/backoff, and wire HNSW consumption in the next service.

## Setup

1. Copy `services/insert_index/.env.example` → `services/insert_index/.env` (fills `SERVICE_BUS_CONN_STR`, `TOPIC_NAME_INGESTION`, `SUBSCRIPTION_NAME_INGESTION`, `TOPIC_NAME_OUTPUT`, `AZURE_STORAGE_CONNECTION_STRING`, `AZURE_STORAGE_ACCOUNT`, `AZURE_VECTOR_CONTAINER`).
2. Install deps: `pip install fastapi uvicorn azure-servicebus azure-storage-blob python-dotenv pydantic numpy scikit-learn hnswlib`.

## How to Run

### Scripted pass

```bash
cd services/insert_index
python - <<'PY'
from fastapi.testclient import TestClient
from server import app

client = TestClient(app)
response = client.post("/create-hnsw")
print(response.json())
PY
```

### API loop

```bash
cd services/insert_index
uvicorn server:app --host 0.0.0.0 --port 8001
curl -X POST "http://localhost:8001/create-hnsw"
```

## Next Steps

- Flesh out `create_hnsw` to build the ANN layer on top of the stored centroids.
- Replace `main.py` with a worker that writes chunk payloads into blob-backed shards while acknowledging Service Bus messages.
- Add logging/metrics + backoff before we run this continuously in prod.
- Provision Azure Blob Storage containers: one for `centroids.npy`/metadata snapshots and another for HNSW shard blobs (plan naming/versioning strategy).
