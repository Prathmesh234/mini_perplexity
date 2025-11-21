# Mini Perplexity

Monorepo skeleton for a Perplexity-style app.

- frontend/: React + TypeScript (Vite)
- backend/: Python backend skeleton
- services/: indexer, embedding, retriever, orchestrator, llm
- data/: raw, processed, indices
- infra/: infra placeholders
- scripts/: utility scripts

## Directory overview

- **backend/** – Python backend using `uv` for dependency management. Install with `uv sync`, and run a dev shell with `uv shell`.
- **frontend/** – React + TypeScript app scaffolded with Vite, ready to extend with additional ESLint/React Compiler options.
- **services/** – Placeholders for the retrieval pipeline (indexer, embedding, retriever, orchestrator, llm, insert_index).
- **data/** – FineWeb ingestion that streams Hugging Face data directly into Azure Blob Storage as gzipped JSONL shards (see `data/README.md` for environment variables).

## Vector storage and retrieval

We store vectors directly in blob storage (S3-compatible buckets) as native binary shards. Each shard is written sequentially so uploads stay simple and append-only, mirroring the approach we use for dataset ingestion. An HNSW (Hierarchical Navigable Small World) index references the object keys: the index holds graph links and vector metadata, while the vectors themselves live in blob storage. Retrieval loads candidates via the HNSW graph, fetches the matching vectors from blob storage, and then re-ranks or filters as needed.

**Why this pattern:**

- Blob storage keeps costs low and scales automatically for growing vector sets.
- HNSW provides fast approximate nearest-neighbor search by navigating layered small-world graphs.
- Decoupling graph metadata (in the index) from vector payloads (in blobs) makes backfills and shard rotations straightforward: regenerate or append shards without rewriting the graph structure.
