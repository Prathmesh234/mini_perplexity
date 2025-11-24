from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from schema import SearchRequest, SearchResponse
from embedding import get_embedding_model
from retrieval import RetrievalEngine
import torch

app = FastAPI(title="Retriever Service")

# Add CORS middleware for local frontend development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In dev, allow all origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize engine
engine = RetrievalEngine()

@app.on_event("startup")
async def startup_event():
    # Pre-load centroids
    try:
        engine.load_centroids()
    except Exception as e:
        print(f"Warning: Could not load centroids on startup: {e}")

    # Pre-load embedding model
    try:
        print("Loading embedding model...")
        get_embedding_model()
        print("Embedding model loaded.")
    except Exception as e:
        print(f"Error loading embedding model: {e}")
        # We might want to raise here if the service is useless without it
        # raise e

@app.post("/search-db", response_model=SearchResponse)
async def search_db(request: SearchRequest):
    try:
        # 1. Embed query
        model = get_embedding_model()
        embedding_tensor = model.embed(request.query)
        # Convert to numpy and flatten
        query_vec = embedding_tensor.cpu().numpy().flatten()
        
        # 2. Search
        results = engine.search(query_vec, k_shards=3, top_n_per_shard=request.k)
        
        # Debug print
        print("\n--- Retrieved Results ---")
        for i, res in enumerate(results):
            print(f"Result {i+1}: {res.text[:100]}...")
        print("-------------------------\n")
        
        return SearchResponse(
            results=results,
            total_candidates=len(results)
        )
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health():
    return {"status": "healthy"}
