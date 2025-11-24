from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn
import httpx
import os
from dotenv import load_dotenv


# Load environment variables
load_dotenv()

app = FastAPI(title="Mini Perplexity Backend")

# Configure CORS to allow frontend requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Frontend dev server
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration
LLM_SERVICE_URL = os.getenv("LLM_SERVICE_URL", "http://localhost:8001/generate")


class SearchRequest(BaseModel):
    query: str


class SearchResponse(BaseModel):
    results: list[str]  # Flexible list of search results
    final_answer: str


@app.get("/")
async def root():
    return {"message": "Mini Perplexity Backend API"}


@app.post("/search", response_model=SearchResponse)
async def search(request: SearchRequest):
    """
    Search endpoint that forwards the request to the LLM service.
    The LLM service handles retrieval and generation.
    """
    try:
        # Call LLM Service
        print(f"Forwarding request to LLM service at: {LLM_SERVICE_URL}")
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                LLM_SERVICE_URL,
                json={"query": request.query, "k": 3},
                timeout=60.0  # LLM generation can take time
            )
        response.raise_for_status()
        data = response.json()
        
        return SearchResponse(
            results=data.get("results", []),
            final_answer=data.get("final_answer", "No answer generated.")
        )
        
    except Exception as e:
        print(f"Error calling LLM service: {e}")
        # Fallback for testing if LLM service is down
        return SearchResponse(
            results=["Error connecting to LLM service", str(e)],
            final_answer="I apologize, but I'm currently unable to connect to the AI service. Please ensure the backend services are running."
        )


def main():
    """Start the FastAPI server"""
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,  # Auto-reload on code changes
        reload_dirs=["."],
        reload_excludes=[".venv"],
        log_level="info"
    )


if __name__ == "__main__":
    main()
