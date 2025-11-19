from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from embedding import EmbeddingModel
import torch
from typing import List

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
