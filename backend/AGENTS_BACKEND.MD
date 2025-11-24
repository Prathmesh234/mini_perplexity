# Backend Service Documentation

## Overview
The backend service will orchestrate the entire search pipeline, acting as the main API gateway between the frontend and the various microservices (embedding, indexer, retriever).

## Current Status
**Not yet implemented.** This document serves as a planning guide for the backend service.

## Planned Architecture

### Responsibilities
1. **API Gateway**: Expose a unified REST API for the frontend.
2. **Query Orchestration**: Coordinate between embedding and retriever services.
3. **Response Formatting**: Structure search results for frontend consumption.
4. **Error Handling**: Provide graceful error responses and logging.
5. **Rate Limiting** (optional): Protect services from abuse.

### Technology Stack (Proposed)
- **Framework**: FastAPI (Python) - for consistency with other services
- **Communication**: HTTP/REST to microservices
- **Deployment**: Docker container, potentially on the same Lambda Labs instance or separate server

### API Endpoints (Planned)

#### `POST /api/search`
**Request:**
```json
{
  "query": "string",
  "k": 5
}
```

**Response:**
```json
{
  "results": [
    {
      "chunk_id": "string",
      "doc_id": "string",
      "score": 0.95,
      "text": "string",
      "metadata": {}
    }
  ],
  "total_results": 5,
  "query_time_ms": 234
}
```

#### `GET /api/health`
Health check endpoint to verify all services are running.

### Integration Points

1. **Retriever Service**: 
   - Endpoint: `http://localhost:8002/search-db`
   - Purpose: Semantic search using vector embeddings

2. **Embedding Service** (if needed separately):
   - Endpoint: `http://localhost:8001/embed`
   - Purpose: Convert text to embeddings (currently handled by retriever)

### Next Steps
1. Create FastAPI application structure
2. Implement `/api/search` endpoint that calls retriever
3. Add error handling and logging
4. Create Dockerfile
5. Add CORS configuration for frontend
6. Implement request/response validation
7. Add basic authentication (optional)

### Environment Variables (Planned)
```
RETRIEVER_SERVICE_URL=http://localhost:8002
EMBEDDING_SERVICE_URL=http://localhost:8001
CORS_ORIGINS=http://localhost:3000
LOG_LEVEL=INFO
```

---

## Notes
- The backend will be the single entry point for the frontend
- Consider adding caching layer for frequent queries
- May need to implement request queuing if load increases
