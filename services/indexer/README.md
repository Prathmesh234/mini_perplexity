# INDEXER

Service for chunking documents and sending them to Azure Service Bus for embedding processing.

## Components

- `chunk_index.py`: Reads files from Azure Blob Storage, parses WET files, and creates Chunk objects
- `embedding_ingestion.py`: Sends Chunk objects to Azure Service Bus topic "ingestion"

## Configuration

Create a `.env` file in this directory with the following variables:

```
# Azure Blob Storage connection string
AZURE_CONN_STR="AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net"

# Azure Blob Storage container name
CONTAINER_NAME="commoncrawl-wet"

# Azure Service Bus connection string
SERVICE_BUS_CONN_STR="Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=..."
```

You can copy `.env.example` to `.env` and fill in your values.

## Usage

```python
from chunk_index import chunk_index
from embedding_ingestion import send_chunks_to_service_bus

# Generate chunks from blob storage
chunks = chunk_index()

# Send chunks to Service Bus topic
sent_count = send_chunks_to_service_bus(chunks)
print(f"Sent {sent_count} chunks to Service Bus")
```
