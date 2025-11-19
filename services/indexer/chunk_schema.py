from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class Chunk(BaseModel):
    """Schema for document chunks."""
    
    chunk: str
    doc_id: str  
    chunk_len: int
    chunk_id: str
    source_file: str
    created_at: Optional[datetime] = None
    
    def __init__(self, **data):
        if 'created_at' not in data:
            data['created_at'] = datetime.utcnow()
        if 'chunk_len' not in data and 'chunk' in data:
            data['chunk_len'] = len(data['chunk'])
        super().__init__(**data)
