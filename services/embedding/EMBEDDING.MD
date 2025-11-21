# Embedding Service Guide

The service exposes a FastAPI application that wraps the Qwen/Qwen3-Embedding-8B model for ad-hoc embedding generation and for the ingestion pipeline. Use the shared helpers in `service_bus.py` and the schema in `publish_schema.py` to keep ingestion payloads consistent.

## Run the Server

1. Copy `services/.env.example` to `services/.env` (or export the env vars manually) and provide:
   - `SERVICE_BUS_CONN_STR`
   - `TOPIC_NAME_INGESTION`
   - Any additional secrets required by the embedding model.
2. From the repository root run:

```bash
cd services/embedding
uvicorn server:app --reload --host 0.0.0.0 --port 8000
```

The server listens on port `8000` by default; override the host/port in the command above if needed.

## Endpoint Overview

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/` | Basic service metadata and a quick reference to available endpoints. |
| `GET` | `/health` | Lightweight health check with the active model name. |
| `POST` | `/embed` | Generate an embedding for a single text string. |
| `POST` | `/start-embedding-process` | Pull a batch of chunks from Service Bus, embed them, and republish with embeddings attached. |

### `GET /`
- **Description:** Returns the service name, model identifier, and a summary of key endpoints.  
- **Sample request:**

```bash
curl http://localhost:8000/
```

### `GET /health`
- **Description:** Confirms the API is running and that the model initialized successfully.  
- **Sample request:**

```bash
curl http://localhost:8000/health
```
- **Sample response:**

```json
{
  "status": "healthy",
  "model": "Qwen/Qwen3-Embedding-8B"
}
```

### `POST /embed`
- **Description:** Generates an embedding vector for arbitrary text. Uses the in-memory `EmbeddingModel` so calls are fast.  
- **Request body:**

```json
{
  "text": "An example chunk or query string"
}
```

- **Success response:**

```json
{
  "embedding": [0.1, -0.23, ...]
}
```

- **Error handling:** Returns HTTP `500` with a descriptive `detail` message when the model fails to generate the embedding.
- **Sample request:**

```bash
curl -X POST http://localhost:8000/embed \
  -H "Content-Type: application/json" \
  -d '{"text":"embed this"}'
```

### `POST /start-embedding-process`
- **Description:** Drives a single pass of the ingestion pipeline:
  1. Pulls pending ingestion messages using `get_ingestion_messages`.
  2. Batches their `chunk` text through the embedding model (leveraging vLLM batching on GPU).
  3. Wraps each embedding using `EmbeddingPublish` and republishes it through `publish_embedding`.
- **Query parameter:** `max_messages` (default `5`) limits how many pending messages are pulled per invocation.
- **Sample request:**

```bash
curl -X POST "http://localhost:8000/start-embedding-process?max_messages=10"
```
- **Success response shape:**

```json
{
  "status": "completed",
  "processed": 3,
  "chunks": [
    {
      "chunk_id": "chunk_1",
      "doc_id": 42,
      "embedding_dimensions": 1536,
      "published": true
    }
  ]
}
```

- **No work to do:** Returns `{"status": "no_messages", "processed": 0}` when Service Bus has nothing pending, or `{"status": "no_valid_messages", "processed": 0}` when messages lack usable text.
- **Error handling:** Emits HTTP `400` for malformed payloads (missing IDs) or `500` when either Service Bus access or embedding generation fails.

## Supporting Modules

- `service_bus.py` supplies `get_ingestion_messages` (batch receive) and `publish_embedding` (publish to `upload-index/uploadindexsub`), loading credentials from the env vars listed above.
- `publish_schema.py` defines `EmbeddingPublish`, ensuring each republished message contains the chunk text, identifiers, length, and embedding vector in a uniform shape.
