from pathlib import Path
import json
from typing import Dict, Any, List, Optional

import numpy as np
from sklearn.cluster import KMeans

from service_bus import receive_all_embeddings
from sampling import sample_embeddings

OUTPUT_DIR = Path(__file__).parent


def create_centroids(
    subscription_name: Optional[str] = None,
    max_messages: int = 3000,
    num_centroids: int = 30,
) -> Dict[str, Any]:
    """
    Fetch embeddings from Service Bus, train k-means on a random sample,
    and persist centroids + routing metadata.
    """
    messages = receive_all_embeddings(
        subscription_name=subscription_name,
        max_messages=max_messages,
    )

    if not messages:
        raise ValueError("No embeddings retrieved from Service Bus.")

    embeddings: List[List[float]] = [
        chunk.embedding for chunk in messages if chunk.embedding is not None
    ]

    if not embeddings:
        raise ValueError("No valid embedding vectors found in messages.")

    # Random sample (reservoir or shuffle)
    embedding_matrix = sample_embeddings(
        embeddings, 
        max_samples=max_messages
    )

    # Train k-means
    kmeans = KMeans(
        n_clusters=num_centroids,
        n_init="auto",
        random_state=42,
    )
    kmeans.fit(embedding_matrix)

    centroids = kmeans.cluster_centers_.astype(np.float32)

    # Persist centroids
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    centroids_path = OUTPUT_DIR / "centroids.npy"
    np.save(centroids_path, centroids)

    # Persist routing metadata (lean)
    metadata = {
        "num_centroids": num_centroids,
        "distance_metric": "l2",
        "shard_assignment_method": "argmin_distance",
        "shard_prefix": "shard_",
        "shards_path": "shards/",
        "centroids_file": "centroids.npy",
        "max_shard_size": 50000
    }

    metadata_path = OUTPUT_DIR / "centroids_metadata.json"
    metadata_path.write_text(json.dumps(metadata, indent=2))

    return {
        "centroids_path": str(centroids_path),
        "metadata_path": str(metadata_path),
        "metadata": metadata,
        "centroids": centroids,
        "chunks": messages,
    }
