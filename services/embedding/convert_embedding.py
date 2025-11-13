import torch
import torch.nn.functional as F
from transformers import AutoTokenizer, AutoModel
from dataclasses import dataclass
from typing import Optional
import numpy as np
import uuid
from datetime import datetime
from pathlib import Path

# Use local model if downloaded, otherwise fallback to HuggingFace Hub
LOCAL_MODEL_PATH = Path(__file__).parent.parent.parent / "models" / "Qwen3-Embedding-8B"
MODEL_NAME = str(LOCAL_MODEL_PATH) if LOCAL_MODEL_PATH.exists() else "Qwen/Qwen3-Embedding-8B"
MAX_LEN = 8192  # from model card

# Global model and tokenizer (loaded once)
tokenizer = None
model = None


@dataclass
class Embedding:
    chunk_str: str
    doc_id: str
    embedding: np.ndarray
    embedding_dim: int
    chunk_id: str
    timestamp: str = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now().isoformat()


def _initialize_model():
    """Initialize the embedding model and tokenizer (lazy loading)."""
    global tokenizer, model
    
    if tokenizer is None or model is None:
        print("Loading Qwen3-Embedding-8B model...")
        tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME, padding_side="left")
        model = AutoModel.from_pretrained(
            MODEL_NAME,
            torch_dtype=torch.float16,
            attn_implementation="flash_attention_2",
            device_map="auto",
        )
        print("Model loaded successfully!")


def _last_token_pool(last_hidden_states: torch.Tensor,
                     attention_mask: torch.Tensor) -> torch.Tensor:
    """Pool the last token from the hidden states."""
    left_padding = (attention_mask[:, -1].sum() == attention_mask.shape[0])
    if left_padding:
        return last_hidden_states[:, -1]
    seq_lengths = attention_mask.sum(dim=1) - 1
    batch_idx = torch.arange(last_hidden_states.size(0), device=last_hidden_states.device)
    return last_hidden_states[batch_idx, seq_lengths]


def embed_sentence(text: str) -> np.ndarray:
    """Convert a sentence to embedding vector."""
    _initialize_model()
    
    batch = tokenizer(
        text,
        padding=True,
        truncation=True,
        max_length=MAX_LEN,
        return_tensors="pt",
    ).to(model.device)

    with torch.no_grad():
        outputs = model(**batch)
        pooled = _last_token_pool(outputs.last_hidden_state, batch["attention_mask"])
        emb = F.normalize(pooled, p=2, dim=1)

    return emb[0].cpu().numpy()  # (D,) numpy vector


def convert_chunk_to_embedding(chunk_text: str, doc_id: str, chunk_id: Optional[str] = None) -> Embedding:
    """
    Convert a text chunk to an Embedding object.
    
    Args:
        chunk_text: The text chunk to embed
        doc_id: Document ID from the service bus message
        chunk_id: Chunk ID from the service bus message (optional, will generate if None)
    
    Returns:
        Embedding object with all required fields
    """
    if chunk_id is None:
        chunk_id = str(uuid.uuid4())
    
    # Generate embedding
    embedding_vector = embed_sentence(chunk_text)
    embedding_dim = embedding_vector.shape[0]
    
    # Create and return Embedding object
    return Embedding(
        chunk_str=chunk_text,
        doc_id=doc_id,
        embedding=embedding_vector,
        embedding_dim=embedding_dim,
        chunk_id=chunk_id
    )


# Example usage
if __name__ == "__main__":
    # Test the embedding function
    test_chunk = "Azure Blob Storage stores unstructured data in the cloud."
    test_doc_id = "test_doc_123"
    
    embedding_obj = convert_chunk_to_embedding(test_chunk, test_doc_id)
    
    print(f"Chunk ID: {embedding_obj.chunk_id}")
    print(f"Doc ID: {embedding_obj.doc_id}")
    print(f"Embedding dimension: {embedding_obj.embedding_dim}")
    print(f"Embedding shape: {embedding_obj.embedding.shape}")
    print(f"Timestamp: {embedding_obj.timestamp}")
    print(f"Chunk preview: {embedding_obj.chunk_str[:50]}...")