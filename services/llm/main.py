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

# Load environment variables
load_dotenv()

app = FastAPI(title="LLM Service with vLLM")

# Configuration
MODEL_NAME = os.getenv("MODEL_NAME", "allenai/Olmo-3-1125-32B")
RETRIEVER_URL = os.getenv("RETRIEVER_URL", "http://localhost:8002")
PORT = int(os.getenv("PORT", "8001"))
HOST = os.getenv("HOST", "0.0.0.0")

print(f"Initializing vLLM with model: {MODEL_NAME}")

# Initialize vLLM engine
# We use AsyncLLMEngine for high throughput
engine_args = AsyncEngineArgs(
    model=MODEL_NAME,
    trust_remote_code=True,  # OLMo usually requires this
    dtype="auto",
    gpu_memory_utilization=0.8,  # Adjust based on available GPU memory
    max_model_len=4096,
)
engine = AsyncLLMEngine.from_engine_args(engine_args)

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
    
    prompt = f"""<|user|>
Answer the user's query based on the provided context. If the answer is not in the context, say you don't know.

Context:
{context_str}

Query:
{request.query}
<|assistant|>
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
