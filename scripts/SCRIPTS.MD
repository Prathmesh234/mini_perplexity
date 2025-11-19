# Scripts

This document tracks operational scripts, commands, and procedures for the search engine pipeline.

## Pipeline Components

### Services Implemented
- **Embedding Service** (`services/embedding/`): vLLM-based embedding generation with FastAPI
- **Indexer Service** (`services/indexer/`): Parallelized chunking and Azure Service Bus publishing

### Key Files Created
```
services/embedding/
├── embedding.py       # vLLM embedding generation
├── server.py          # FastAPI server for /embed endpoint
├── main.py            # Server startup script
├── .env.example       # Environment configuration template
└── pyproject.toml     # Dependencies: vllm, torch, fastapi, uvicorn

services/indexer/
├── worker.py          # Azure Blob Storage reader and chunker
├── indexer.py         # Parallel processing orchestrator (4 workers)
├── service_bus.py     # Azure Service Bus publisher
├── chunk_schema.py    # Chunk data schema definition
├── server.py          # FastAPI control server
├── main.py            # Server startup script
├── .env.example       # Environment configuration template
└── pyproject.toml     # Dependencies and configuration
```

## Operational Commands

### Starting Services

**Embedding Service:**
```bash
cd services/embedding
uvicorn server:app --host 0.0.0.0 --port 8001
# Or via main.py:
python main.py
```

**Indexer Service:**
```bash
cd services/indexer
uvicorn server:app --host 0.0.0.0 --port 8002
# Or via main.py:
python main.py
```

### API Usage

**Embedding Generation:**
```bash
# Generate embeddings
curl -X POST "http://localhost:8001/embed" \
  -H "Content-Type: application/json" \
  -d '{"text": "Your text to embed"}'
```

**Indexer Control:**
```bash
# Start indexing process
curl -X POST "http://localhost:8002/start-indexing" \
  -H "Content-Type: application/json" \
  -d '{"container": "commoncrawl-wet", "prefix": "fineweb/train/"}'

# Check indexing status
curl "http://localhost:8002/status"

# Stop indexing
curl -X POST "http://localhost:8002/stop-indexing"
```

### Process Monitoring

**Check Running Processes:**
```bash
# List all Python processes
ps aux | grep python

# List processes by service
ps aux | grep "embedding\|indexer"

# Monitor resource usage
top -p $(pgrep -f "uvicorn\|python.*main\.py" | tr '\n' ',')
```

**Monitor Parallel Workers:**
```bash
# Find parent indexer process
ps aux | grep "indexer.py\|server.py.*8002"

# List all child processes of indexer
ps --forest -o pid,ppid,cmd -g $(pgrep -f "indexer")

# Monitor parallel processing
watch -n 2 'ps aux | grep "worker.py\|IndexerWorker"'
```

### Process Control

**Stop Services:**
```bash
# Stop by process name
pkill -f "uvicorn.*8001"  # Stop embedding service
pkill -f "uvicorn.*8002"  # Stop indexer service

# Stop all parallel workers
pkill -f "IndexerWorker\|worker.py"

# Force stop if needed
pkill -9 -f "uvicorn\|python.*main\.py"
```

**Graceful Shutdown:**
```bash
# Find PIDs first
ps aux | grep "uvicorn\|main\.py" | grep -v grep

# Send SIGTERM for graceful shutdown
kill $(pgrep -f "uvicorn.*server:app")
```

## Azure Configuration

### Required Environment Variables

**Embedding Service:**
```env
AZURE_OPENAI_ENDPOINT=your_endpoint
AZURE_OPENAI_KEY=your_key
MODEL_NAME=your_model_name
HF_HOME=/path/to/cache
```

**Indexer Service:**
```env
AZURE_STORAGE_CONNECTION_STRING=your_connection_string
AZURE_SERVICE_BUS_CONNECTION_STRING=your_service_bus_connection
SERVICE_BUS_TOPIC_NAME=ingestion
SERVICE_BUS_NAMESPACE=embedding-pipeline-search
CONTAINER_NAME=commoncrawl-wet
CHUNK_SIZE=1000
MAX_WORKERS=4
```

### Azure Resources Setup
- **Blob Storage Container**: `commoncrawl-wet` with `fineweb/train/` prefix
- **Service Bus Namespace**: `embedding-pipeline-search`
- **Service Bus Topic**: `ingestion` for chunk publishing

## Data Pipeline Architecture

### Processing Flow
1. **Data Source**: FineWeb dataset in Azure Blob Storage (`fineweb/train/`)
2. **Parallel Processing**: 4 worker processes, each handling unique file ranges
3. **Chunking**: Text split by paragraphs with configurable chunk size (default: 1000 chars)
4. **Publishing**: Each chunk sent individually to Azure Service Bus topic
5. **Schema**: Standardized chunk format with metadata (doc_id, chunk_id, source_file, timestamps)

### Chunk Schema
```json
{
  "chunk": "text content",
  "doc_id": "unique_document_id", 
  "chunk_len": 1000,
  "chunk_id": "chunk_identifier",
  "source_file": "path/to/source/file",
  "created_at": "2024-01-01T00:00:00Z"
}
```

## Debugging and Troubleshooting

### Common Issues Fixed
1. **Container Name**: Changed from `fineweb` to `commoncrawl-wet`
2. **File Path**: Corrected to `fineweb/train/` prefix
3. **Gzip Decompression**: Fixed usage of `gzip.decompress()` for .gz files
4. **Process Visibility**: Ensured parallel workers are trackable via ps commands

### Log Monitoring
```bash
# Real-time log monitoring
tail -f logs/embedding.log
tail -f logs/indexer.log

# Search for specific errors
grep -i "error\|exception" logs/*.log
```

### Health Checks
```bash
# Service health
curl "http://localhost:8001/health" 2>/dev/null || echo "Embedding service down"
curl "http://localhost:8002/health" 2>/dev/null || echo "Indexer service down"

# Azure connectivity
curl "http://localhost:8002/status"  # Check Azure Blob/Service Bus connectivity
```

## Performance Monitoring

### Metrics to Track
- **Throughput**: Files processed per minute
- **Chunk Rate**: Chunks generated and published per second
- **Resource Usage**: CPU, memory usage across workers
- **Error Rate**: Failed processing attempts
- **Queue Depth**: Service Bus topic message backlog

### Scaling Considerations
- Adjust `MAX_WORKERS` based on available CPU cores
- Monitor Azure Service Bus quotas and throttling
- Blob Storage bandwidth limits
- vLLM model loading time and memory requirements

## Change Log

### 2024-01-XX - Initial Implementation
- Created embedding service with vLLM integration
- Implemented parallel indexer with Azure Blob Storage integration
- Set up Service Bus publishing pipeline
- Added comprehensive monitoring and operational scripts
- Debugged container naming, file paths, and gzip handling
- Updated .gitignore for Python, model weights, and cloud artifacts
