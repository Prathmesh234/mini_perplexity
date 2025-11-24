# Retriever Service Documentation

## Overview
The Retriever Service is a high-performance semantic search engine component. It is responsible for:
1.  **Embedding**: Converting user queries into vector embeddings using a GPU-accelerated LLM (`Qwen/Qwen3-Embedding-8B`).
2.  **Routing**: Determining which index shards contain relevant data using a centroid-based routing mechanism.
3.  **Retrieval**: Searching specific HNSW (Hierarchical Navigable Small World) index shards to find the most similar document chunks.
4.  **Artifact Management**: Automatically downloading necessary index artifacts (centroids, shards) from Azure Blob Storage on demand.

## File Structure & Component Breakdown

### `main.py`
-   **Purpose**: The entry point for the service.
-   **Function**: It uses `subprocess` to launch the FastAPI server via `uvicorn`. This ensures the server runs in the correct Python environment.

### `server.py`
-   **Purpose**: The FastAPI application definition.
-   **Key Components**:
    -   `POST /search-db`: The main endpoint. Accepts a query, embeds it, and returns search results.
    -   `startup_event`: Initializes the `RetrievalEngine` (loading centroids) and pre-loads the `EmbeddingModel` to GPU memory to ensure the service is warm and ready.

### `embedding.py`
-   **Purpose**: Wraps the vLLM library to handle text embedding.
-   **Key Components**:
    -   `EmbeddingModel` class: Initializes the `Qwen` model with optimized GPU memory settings (`gpu_memory_utilization=0.7`).
    -   `get_embedding_model()`: Implements a **singleton pattern** to ensure the heavy model is only loaded once and reused across requests.

### `retrieval.py`
-   **Purpose**: Contains the core logic for semantic search.
-   **Key Components**:
    -   `RetrievalEngine` class:
        -   `load_centroids()`: Loads the centroid vectors used for routing.
        -   `find_nearest_shards()`: Compares the query vector against centroids to find the most relevant shards (clusters).
        -   `search_shard()`: Downloads (if missing) and searches a specific HNSW index shard. It maps integer labels back to text using `ids.json`.
        -   `search()`: Orchestrates the process: Query -> Centroids -> Shards -> Results.

### `blob_storage.py`
-   **Purpose**: Handles all interactions with Azure Blob Storage.
-   **Key Components**:
    -   `download_centroids()`: Fetches the global `centroids.npy`.
    -   `download_shard_artifacts()`: Fetches the 4 key files for a shard: `index.bin` (HNSW graph), `ids.json` (metadata), `vectors.npy`, and `meta.json`.
    -   **Configuration**: Reads `.env` variables to determine the correct container (`vectorindexes`) and prefix (`vector-indexes-client1`).

### `schema.py`
-   **Purpose**: Defines the data models (Pydantic) for API requests and responses.
-   **Models**: `SearchRequest` (query, k) and `SearchResponse` (list of `SearchResult`).

### `cache_manager.py`
-   **Purpose**: Background process that manages disk space by implementing LRU (Least Recently Used) cache eviction.
-   **Key Components**:
    -   Runs as a separate subprocess launched by `main.py`.
    -   Monitors `/tmp/retriever_cache` every 60 seconds.
    -   If cache size exceeds 2GB, it deletes the oldest shards (based on modification time).
    -   Works in tandem with `retrieval.py`, which "touches" shard directories on access to update their timestamps.

## Architecture & Data Flow

1.  **Query Arrives**: Frontend sends `POST /search-db` with a text query.
2.  **Embedding**: The query is converted to a vector using the pre-loaded Qwen model (GPU-accelerated).
3.  **Routing**: The query vector is compared against centroids to find the top 3 nearest shards.
4.  **Lazy Loading**: For each shard:
    -   Check if it exists locally in `/tmp/retriever_cache/shard_XXX`.
    -   If not, download `index.bin`, `ids.json`, `vectors.npy`, and `meta.json` from Azure.
    -   Update the shard's timestamp (via `touch()`) to mark it as recently used.
5.  **Search**: Load the HNSW index and perform ANN search to find the top-k nearest vectors.
6.  **Mapping**: Convert HNSW integer labels to actual text chunks using `ids.json`.
7.  **Response**: Return the results to the frontend.
8.  **Background Cleanup**: `cache_manager.py` periodically checks cache size and evicts old shards if needed.

## Concurrency & Performance

-   **Multiple Users**: FastAPI handles concurrent requests, but the GPU processes embeddings sequentially (queued by vLLM).
-   **Cache Strategy**: LRU eviction ensures frequently accessed shards stay in memory while old ones are removed.
-   **CORS Enabled**: Allows local frontend development via SSH tunnel to Lambda Labs GPU server.

---

## Debugging Session Log (Nov 24, 2025)

During the initial setup, we encountered and resolved several critical issues to get the service running.

### 1. Centroids Loading Failure
**Error:** `Warning: Could not load centroids on startup: Blob centroids.npy not found`
**Cause:** The code assumed a flat container structure, but the actual structure in Azure had a specific container (`vectorindexes`) and a prefix directory (`vector-indexes-client1`).
**Mitigation:** 
- Updated `blob_storage.py` to correctly read `AZURE_VECTOR_CONTAINER` and `AZURE_BLOB_PREFIX` from `.env`.

### 2. Shard Download 404s
**Error:** `Warning: shards/shard_24/index.bin not found` (404 Not Found)
**Cause:** The code generated shard paths using unpadded integers (e.g., `shard_24`), but the Azure Blob Storage folders used zero-padded 3-digit format (e.g., `shard_024`).
**Mitigation:** 
- Updated `retrieval.py` and `blob_storage.py` to format shard IDs using `f"shard_{shard_id:03d}"`.

### 3. Local Directory Mismatch
**Error:** Shards were downloaded to `shard_24` (initially) but the retriever looked for `shard_024`, or vice versa.
**Cause:** Inconsistent naming conventions between the downloader and the reader functions.
**Mitigation:** 
- Standardized both `download_shard_artifacts` and `search_shard` to use the zero-padded `shard_{shard_id:03d}` format for local directory names.

### 4. vLLM OOM / Initialization Crash
**Error:** `ValueError: Free memory on device ... is less than desired GPU memory utilization`
**Cause:** vLLM defaults to 90% GPU memory utilization, which was too high for the available VRAM (approx 22GB free of 80GB).
**Mitigation:** 
- Reduced `gpu_memory_utilization` to `0.7` in `embedding.py`.
- Ensured the embedding model is pre-loaded as a singleton during the FastAPI `startup` event.

### 5. ID Map Parsing Error
**Error:** `AttributeError: 'list' object has no attribute 'keys'` when searching.
**Cause:** The `ids.json` file format was a **list of objects** (e.g., `[{"label": 0, ...}]`), but the code expected a **dictionary** (e.g., `{"0": ...}`).
**Mitigation:** 
- Updated `retrieval.py` to check the type of loaded JSON. If it's a list, it converts it into a dictionary keyed by the `label` field for O(1) lookup.
