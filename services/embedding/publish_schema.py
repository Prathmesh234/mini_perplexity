from typing import List, Union
from pydantic import BaseModel, Field


class EmbeddingPublish(BaseModel):
    """Schema for payload published after embedding generation."""

    chunk: str
    chunk_id: Union[str, int] = Field(..., description="Identifier for the chunk")
    doc_id: int = Field(..., description="Document identifier")
    chunk_len: int
    embedding: List[float]
