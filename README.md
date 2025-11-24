# Mini Perplexity

Object Storage based vector database + RAG solution. We used fineweb for out index dataset. 

Cached (Shard has been cached in the cache_dir of the server)

https://github.com/user-attachments/assets/8899e3db-37a3-46cc-a856-5c4c4bf0f287



Non Cached (Fresh shard pulled from object storage)



https://github.com/user-attachments/assets/4832c595-517b-4194-9741-f0286de0685c





Monorepo skeleton for a Perplexity-style app.

- frontend/: React + TypeScript (Vite)
- backend/: Python backend skeleton
- services/: indexer, embedding, retriever, orchestrator, llm
- data/: raw, processed, indices
- infra/: infra placeholders
- scripts/: utility scripts

## Directory overview

- **backend/** – Python FastAPI backend using `uv` for dependency management. Install with `uv sync`, and run a dev shell with `uv shell`.
- **frontend/** – React + TypeScript app scaffolded with Vite.
- **services/** – End-to-end retrieval pipeline:
  - **insert_index/** – Turns streaming embeddings into centroids, shards, and HNSW artifacts pushed directly to blob storage.
  - **retriever/** – Loads centroids, routes queries to the nearest shards, downloads HNSW artifacts on-demand, and returns ranked results.
  - **llm/** – Placeholder for generation/reranking once retrieval returns candidate chunks.
- **data/** – FineWeb ingestion that streams Hugging Face data directly into Azure Blob Storage as gzipped JSONL shards (see `data/README.md` for environment variables).
- **infra/** – infra placeholders
- **scripts/** – utility scripts

## Object-storage-native vector database

We treat blob storage as the persistence layer for everything related to retrieval:

- **Centroids** – Stored as `centroids.npy` alongside lightweight routing metadata. They partition the vector space and decide which shard receives a given embedding.
- **Shards** – One directory per centroid (for example, `shards/shard_007/`). Each shard is append-only and contains:
  - `vectors.npy` – The raw float32 vectors assigned to that centroid.
  - `index.bin` – An HNSW graph built over the shard’s vectors.
  - `ids.json` – A label → `{chunk_id, doc_id, chunk_text, chunk_len}` mapping used to translate ANN hits back to source text.
  - `meta.json` – HNSW parameters, centroid coordinates, counts, and blob paths.
- **Blob-first writes** – Shard artifacts are serialized in-memory and streamed directly into the blob container; no intermediate filesystem writes are required on the writer.

**Why this pattern:**

- Blob storage keeps costs low and scales automatically for growing vector sets.
- HNSW provides fast approximate nearest-neighbor search by navigating layered small-world graphs.
- Decoupling graph metadata (in the index) from vector payloads (in blobs) makes backfills and shard rotations straightforward: regenerate or append shards without rewriting the graph structure.

## How the index is built (services/insert_index)

The `insert_index` service ingests embeddings published to Azure Service Bus and turns them into blob-backed shards:

1. **Ingestion** (`service_bus.py`) – Batches up to 3k messages from the ingestion topic, validating each payload as an `EmbeddingChunk`.
2. **Sampling** (`sampling.py`) – Deterministically shuffles and truncates embeddings to a training subset.
3. **Centroid training** (`centroids.py`) – Runs k-means to produce `num_centroids` cluster centers; writes `centroids.npy` and `centroids_metadata.json` describing shard prefixes and distance metrics.
4. **Shard construction** (`shards.py`) – Assigns every embedding to its nearest centroid, builds an HNSW index per centroid with tuned `M/ef_construction/ef_runtime`, and serializes `index.bin`, `vectors.npy`, `ids.json`, and shard `meta.json` entirely in-memory.
5. **Object storage upload** (`store_blob.py`) – Streams the centroids and every shard artifact directly into the configured blob container/prefix. No files are kept locally after upload.

The service exposes `POST /create-hnsw` (`server.py`) to run this pipeline end-to-end for manual testing; a long-running worker can reuse the same modules to keep appending shards.

## How retrieval works (services/retriever)

The retriever is a FastAPI service that performs embedding, routing, and shard-level ANN search:

1. **Query embedding** (`embedding.py`) – Uses a GPU-backed vLLM wrapper to encode the query; loaded once on startup for warm responses.
2. **Centroid routing** (`retrieval.py`) – Loads `centroids.npy` (downloads it if missing) and finds the nearest centroid IDs for the query vector. These centroid indices map directly to shard IDs (`shard_{centroid_idx:03d}`).
3. **Lazy shard fetch** (`blob_storage.py`) – For each routed shard, download `index.bin`, `vectors.npy`, `ids.json`, and `meta.json` into `/tmp/retriever_cache` if not already present. Access timestamps are touched so the cache manager can implement LRU eviction.
4. **HNSW search** (`retrieval.py`) – Loads the shard’s HNSW index, runs ANN search, and remaps integer labels back to chunk text via `ids.json`. Scores are derived from distances and results across shards are merged/sorted.
5. **Cache hygiene** (`cache_manager.py`) – A lightweight subprocess deletes the least recently used shard directories when cache size exceeds the configured cap (default 2GB).

This separation lets writers stream new shards into object storage while the retriever downloads only what it needs for live queries.
