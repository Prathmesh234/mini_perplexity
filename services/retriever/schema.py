from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any

class SearchRequest(BaseModel):
    query: str
    k: int = Field(default=3, description="Number of results to return")

class SearchResult(BaseModel):
    chunk_id: str
    doc_id: Any
    score: float
    text: str
    metadata: Optional[Dict[str, Any]] = None

class SearchResponse(BaseModel):
    results: List[SearchResult]
    total_candidates: int
