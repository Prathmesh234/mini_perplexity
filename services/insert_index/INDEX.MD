# Insert Index Service Snapshot

This package turns Service Bus ingestion batches into k-means centroids that we will later shard across Azure Blob Storage. Everything relevant to the centroid workflow is captured below.

## Modules

- `schema.py` – `EmbeddingChunk` (chunk text/ids/length + embedding) used to validate every message.
- `service_bus.py`
  - `load_env_config()` loads `.env` (connection string + topic names).
  - `receive_all_embeddings()` drains batches of up to 3k messages, validates them as `EmbeddingChunk`, and returns the objects for downstream sampling.
  - `_deserialize_message_body()`/`publish_chunk()` handle JSON parsing + optional re-publish hooks.
- `sampling.py` – `sample_embeddings()` shuffles deterministically, truncates to 3k, and returns a float32 NumPy matrix.
- `centroids.py` – `create_centroids(subscription_name, max_messages, num_centroids=30)` pulls embeddings, samples, trains k-means (`random_state=42`), writes `centroids.npy`, and emits lightweight routing metadata (`centroids_metadata.json`).
- `server.py`
  - `POST /create-centroids`: synchronous loop that keeps requesting batches of 3000 until embeddings exist; on success returns `"status": "completed"` plus file paths, and when the topic is empty it breaks with `"status": "empty"` (commented as temporary behavior).
  - `POST /create-hnsw`: placeholder for the next index-building phase.
  - `GET /health`: simple readiness probe.
- `main.py` – still a stub; replace with the blob-backed insertion worker later.

## Setup

1. Copy `services/.env.example` → `services/insert_index/.env` (need `SERVICE_BUS_CONN_STR`, `TOPIC_NAME_INGESTION`, optional `TOPIC_NAME_OUTPUT`).
2. Install deps: `pip install fastapi uvicorn azure-servicebus python-dotenv pydantic numpy scikit-learn`.

## How to Run

### Scripted pass

```bash
cd services/insert_index
python - <<'PY'
from centroids import create_centroids
print(create_centroids(max_messages=3000, subscription_name="ingestion-sub"))
PY
```

### API loop

```bash
cd services/insert_index
uvicorn server:app --host 0.0.0.0 --port 8001
curl -X POST "http://localhost:8001/create-centroids?subscription_name=ingestion-sub"
```

## Next Steps

- Flesh out `create_hnsw` to build the ANN layer on top of the stored centroids.
- Replace `main.py` with a worker that writes chunk payloads into blob-backed shards while acknowledging Service Bus messages.
- Add logging/metrics + backoff before we run this continuously in prod.
- Provision Azure Blob Storage containers: one for `centroids.npy`/metadata snapshots and another for HNSW shard blobs (plan naming/versioning strategy).
