# Retriever

FastAPI service that embeds user queries, routes them to the right shards via centroids, and runs HNSW search over blob-backed artifacts.

## Request flow

1. **Startup** – `server.py` loads `centroids.npy` (downloading from blob storage if missing) and warms the embedding model.
2. **Embedding** – `embedding.py` wraps vLLM to turn the request text into a vector.
3. **Routing** – `retrieval.py` compares the query vector against centroids to choose the nearest shards (`shard_{centroid_id:03d}`).
4. **Shard fetch** – `blob_storage.py` downloads missing shard artifacts (`index.bin`, `ids.json`, `vectors.npy`, `meta.json`) into `/tmp/retriever_cache`; `cache_manager.py` trims this cache with an LRU policy.
5. **Search** – `retrieval.py` loads the shard HNSW index, executes ANN search, and remaps labels to chunk text using `ids.json` before returning ranked results.

Blob storage remains the source of truth; shards are only cached locally to serve queries quickly.
