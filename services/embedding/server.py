from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from embedding import EmbeddingModel
import torch
from typing import List, Dict, Any

from service_bus import get_ingestion_messages, publish_embedding
from publish_schema import EmbeddingPublish

# Initialize FastAPI app
app = FastAPI(title="Embedding Service", version="1.0.0")

# Initialize embedding model once at startup
embedding_model = EmbeddingModel()

class EmbedRequest(BaseModel):
    text: str

class EmbedResponse(BaseModel):
    embedding: List[float]

@app.post("/embed", response_model=EmbedResponse)
async def embed_text(request: EmbedRequest):
    """
    Generate embedding for input text.
    
    Args:
        request: Contains text to embed
        
    Returns:
        EmbedResponse: Contains the embedding vector
    """
    try:
        # Generate embedding
        embedding_tensor = embedding_model.embed(request.text)
        
        # Convert to list of floats
        embedding_list = embedding_tensor.squeeze().tolist()
        
        return EmbedResponse(embedding=embedding_list)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating embedding: {str(e)}")


@app.post("/start-embedding-process")
async def start_embedding_process(max_messages: int = 5) -> Dict[str, Any]:
    """
    Trigger a single pass of the embedding pipeline by pulling messages from Service Bus.
    """
    try:
        pending_messages = get_ingestion_messages(max_message_count=max_messages)
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    if not pending_messages:
        return {"status": "no_messages", "processed": 0}

    valid_entries = []
    text_batches: List[str] = []

    for message in pending_messages:
        chunk_text = message.get("chunk") or message.get("text")
        if not chunk_text:
            continue

        valid_entries.append({"message": message, "chunk_text": chunk_text})
        text_batches.append(chunk_text)

    if not valid_entries:
        return {"status": "no_valid_messages", "processed": 0}

    try:
        # vLLM batches the requests internally, executing them concurrently on the GPU.
        embeddings_tensor = embedding_model.embed(text_batches)
        embedding_results = embeddings_tensor.tolist()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Error generating embeddings: {exc}")

    processed_chunks = []

    for entry, embedding_list in zip(valid_entries, embedding_results):
        message = entry["message"]
        chunk_text = entry["chunk_text"]

        chunk_id = message.get("chunk_id") or message.get("id")
        if chunk_id is None:
            raise HTTPException(status_code=400, detail="Chunk ID missing in message payload")

        doc_id_raw = message.get("doc_id")
        if doc_id_raw is None:
            raise HTTPException(status_code=400, detail="doc_id missing in message payload")

        try:
            doc_id_value = int(doc_id_raw)
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="doc_id must be convertible to int")

        chunk_len_value = message.get("chunk_len") or len(chunk_text)

        embedding_payload = EmbeddingPublish(
            chunk=chunk_text,
            chunk_id=chunk_id,
            doc_id=doc_id_value,
            chunk_len=int(chunk_len_value),
            embedding=embedding_list,
        )

        published = publish_embedding(embedding_payload)

        processed_chunks.append({
            "chunk_id": chunk_id,
            "doc_id": doc_id_value,
            "embedding_dimensions": len(embedding_list),
            "published": published
        })

    return {
        "status": "completed",
        "processed": len(processed_chunks),
        "chunks": processed_chunks
    }

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "model": "Qwen/Qwen3-Embedding-4B"}

@app.get("/")
async def root():
    """Root endpoint with basic info."""
    return {
        "service": "Embedding Service",
        "model": "Qwen/Qwen3-Embedding-4B",
        "endpoints": {
            "embed": "POST /embed - Generate embedding for text",
            "health": "GET /health - Health check"
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
