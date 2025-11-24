import os
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional
import httpx
from dotenv import load_dotenv
from vllm import AsyncLLMEngine
from vllm.engine.arg_utils import AsyncEngineArgs
from vllm.sampling_params import SamplingParams
import uuid

from contextlib import asynccontextmanager

# Load environment variables
load_dotenv()

# Configuration
MODEL_NAME = os.getenv("MODEL_NAME", "allenai/Olmo-3-1125-32B")
RETRIEVER_URL = os.getenv("RETRIEVER_URL", "http://localhost:8002")
PORT = int(os.getenv("PORT", "8001"))
HOST = os.getenv("HOST", "0.0.0.0")

# Global engine variable
engine = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global engine
    print(f"Initializing vLLM with model: {MODEL_NAME}")
    
    # Initialize vLLM engine
    # We use AsyncLLMEngine for high throughput
    engine_args = AsyncEngineArgs(
        model=MODEL_NAME,
        trust_remote_code=True,  # OLMo usually requires this
        dtype="auto",
        gpu_memory_utilization=0.8,  # Adjust based on available GPU memory
        max_model_len=32768,  # Increased for H100 80GB - plenty of KV cache space
    )
    engine = AsyncLLMEngine.from_engine_args(engine_args)
    
    yield
    
    # Clean up if needed (vLLM usually handles cleanup on process exit)
    print("Shutting down vLLM service")

app = FastAPI(title="LLM Service with vLLM", lifespan=lifespan)

class GenerateRequest(BaseModel):
    query: str
    k: int = 3

class GenerateResponse(BaseModel):
    final_answer: str
    results: List[str]

@app.post("/generate", response_model=GenerateResponse)
async def generate(request: GenerateRequest):
    request_id = str(uuid.uuid4())
    
    # 1. Call Retriever Service
    print(f"[{request_id}] Calling retriever for query: {request.query}")
    try:
        async with httpx.AsyncClient() as client:
            retriever_response = await client.post(
                f"{RETRIEVER_URL}/search-db",
                json={"query": request.query, "k": request.k},
                timeout=10.0
            )
            retriever_response.raise_for_status()
            search_data = retriever_response.json()
        
        # Extract chunk texts
        # SearchResponse has 'results' list of SearchResult objects which have 'text' field
        results_list = search_data.get("results", [])
        chunk_texts = [res.get("text", "") for res in results_list]
        
        print(f"[{request_id}] Retrieved {len(chunk_texts)} chunks")
        
    except Exception as e:
        print(f"[{request_id}] Error calling retriever: {e}")
        # Fallback: empty context if retriever fails
        chunk_texts = []

    # 2. Construct Prompt
    context_str = "\n\n".join([f"Source {i+1}: {text}" for i, text in enumerate(chunk_texts)])
    
    prompt = f"""
You are given a context passage and a user query. Your task is to answer the query
**strictly and exclusively** using information found in the provided context.

Compliance Requirements:
- Only answer the user’s question **directly**. No introductions, no explanations, no extra sentences.
- If the context contains the answer, provide a concise, precise response drawn solely from the context.
- If the answer is not fully supported by the context, reply exactly with: "I don't know."
- Do NOT infer, assume, or use any outside knowledge.
- Do NOT restate or reference the context, the query, or these rules in the answer.
- Do NOT add formatting, bullet points, reasoning, or commentary.
- Output only the final answer.

Context:
{context_str}

Query:
{request.query}


"""
    
    # 3. Generate with vLLM
    sampling_params = SamplingParams(
        temperature=1.0,
        top_p=0.7,
        max_tokens=1024,
    )
    
    print(f"[{request_id}] Generating answer...")
    results_generator = engine.generate(prompt, sampling_params, request_id)
    
    # Get the final result
    final_output = None
    async for request_output in results_generator:
        final_output = request_output
        
    generated_text = final_output.outputs[0].text
    print(f"[{request_id}] Generation complete")

    return GenerateResponse(
        final_answer=generated_text,
        results=chunk_texts
    )

@app.get("/health")
async def health():
    return {"status": "healthy", "model": MODEL_NAME}

def main():
    uvicorn.run(app, host=HOST, port=PORT)

if __name__ == "__main__":
    main()
