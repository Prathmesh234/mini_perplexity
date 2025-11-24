from typing import List, Union
import torch

class EmbeddingModel:
    def __init__(self, model_name: str = "Qwen/Qwen3-Embedding-8B"):
        from vllm import LLM
        # Use a smaller max_num_seqs for the retriever since we process single queries
        self.model = LLM(model=model_name, task="embed", max_num_seqs=16, enforce_eager=True, gpu_memory_utilization=0.7)
    
    def embed(self, texts: Union[str, List[str]]) -> torch.Tensor:
        if isinstance(texts, str):
            texts = [texts]
            
        outputs = self.model.embed(texts)
        
        # Extract embeddings from outputs
        embeddings = [output.outputs.embedding for output in outputs]
        
        return torch.tensor(embeddings)

# Global instance
_model_instance = None

def get_embedding_model():
    global _model_instance
    if _model_instance is None:
        _model_instance = EmbeddingModel()
    return _model_instance
