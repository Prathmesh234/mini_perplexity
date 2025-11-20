from typing import List, Union
from pydantic import BaseModel, Field


class EmbeddingChunk(BaseModel):
    """Schema for raw chunks flowing through the ingestion pipeline."""

    chunk: str
    chunk_id: Union[str, int] = Field(..., description="Identifier for the chunk")
    doc_id: Union[int, str] = Field(..., description="Document identifier")
    chunk_len: int
    embedding: List[float] = Field(..., description="Vector representation of the chunk")
