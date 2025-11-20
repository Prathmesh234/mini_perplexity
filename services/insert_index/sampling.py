from typing import List, Sequence
import random

import numpy as np


def sample_embeddings(
    embeddings: Sequence[Sequence[float]],
    max_samples: int = 3000,
    seed: int = 42,
) -> np.ndarray:
    """
    Shuffle embeddings deterministically and return up to `max_samples` as float32 matrix.
    """
    embeddings_list: List[Sequence[float]] = list(embeddings)
    if not embeddings_list:
        raise ValueError("No embeddings available for sampling")

    rng = random.Random(seed)
    rng.shuffle(embeddings_list)

    limit = min(len(embeddings_list), max_samples)
    selected = embeddings_list[:limit]

    matrix = np.asarray(selected, dtype=np.float32)
    if matrix.ndim != 2:
        raise ValueError("Embeddings must form a 2D matrix")

    return matrix
