import torch
from typing import List, Union

def convert_embeddings(
    text_chunks: Union[str, List[str]], 
    model_name: str = "Qwen/Qwen3-Embedding-8B"
) -> torch.Tensor:
    """
    Convert text chunk(s) into embeddings using vLLM.
    
    Args:
        text_chunks: Single text or list of texts to embed
        model_name: HuggingFace model identifier
    
    Returns:
        torch.Tensor: Embedding vector(s)
    """
    try:
        from vllm import LLM
        
        # Ensure input is a list
        if isinstance(text_chunks, str):
            text_chunks = [text_chunks]
        
        # Initialize model (downloads automatically on first run)
        model = LLM(model=model_name, task="embed", max_num_seqs=512)
        
        # Generate embeddings
        outputs = model.embed(text_chunks)
        
        # Extract embeddings correctly
        embeddings = torch.tensor([o.outputs.embedding for o in outputs])
        
        return embeddings
        
    except ImportError:
        raise ImportError("vLLM not installed. Install with: pip install vllm>=0.8.5")
    except Exception as e:
        raise RuntimeError(f"Error generating embedding: {e}")

# For production: Initialize once outside function
class EmbeddingModel:
    def __init__(self, model_name: str = "Qwen/Qwen3-Embedding-8B"):
        from vllm import LLM
        self.model = LLM(model=model_name, task="embed", max_num_seqs=256)
    
    def embed(self, texts: Union[str, List[str]]) -> torch.Tensor:
        if isinstance(texts, str):
            texts = [texts]
        outputs = self.model.embed(texts)
        return torch.tensor([o.outputs.embedding for o in outputs])
