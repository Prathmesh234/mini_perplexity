import numpy as np
import hnswlib
import json
import os
from pathlib import Path
from typing import List, Dict, Any, Tuple
from schema import SearchResult
from blob_storage import download_centroids, download_shard_artifacts

class RetrievalEngine:
    def __init__(self, cache_dir: str = "/tmp/retriever_cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.centroids = None
        self.centroids_path = self.cache_dir / "centroids.npy"
        
    def load_centroids(self):
        """Load centroids from disk or download if missing."""
        if not self.centroids_path.exists():
            print("Downloading centroids...")
            download_centroids(self.cache_dir)
            
        self.centroids = np.load(self.centroids_path)
        print(f"Loaded {len(self.centroids)} centroids.")

    def find_nearest_shards(self, query_embedding: np.ndarray, k: int = 1) -> List[int]:
        """
        Find the k nearest centroids to the query embedding.
        Returns a list of shard IDs (indices of centroids).
        """
        if self.centroids is None:
            self.load_centroids()
            
        # Compute cosine similarity (dot product of normalized vectors)
        # Assuming query_embedding and centroids are already normalized or we want L2?
        # Centroids from sklearn KMeans are usually Euclidean.
        # Let's use L2 distance for consistency with KMeans.
        
        dists = np.linalg.norm(self.centroids - query_embedding, axis=1)
        nearest_indices = np.argsort(dists)[:k]
        return nearest_indices.tolist()

    def search_shard(self, shard_id: int, query_embedding: np.ndarray, top_n: int = 5) -> List[SearchResult]:
        """
        Search within a specific shard.
        """
        shard_dir = self.cache_dir / f"shard_{shard_id:03d}"
        index_path = shard_dir / "index.bin"
        ids_path = shard_dir / "ids.json"
        
        # Download if missing
        print(f"Checking shard {shard_id:03d} artifacts at {shard_dir}...")
        if not index_path.exists() or not ids_path.exists():
            print(f"Downloading shard {shard_id:03d} because index_exists={index_path.exists()}, ids_exists={ids_path.exists()}")
            download_shard_artifacts(shard_id, self.cache_dir)
        else:
            print(f"Shard {shard_id:03d} artifacts found locally.")
            
        if not index_path.exists():
            print(f"Shard {shard_id:03d} not found/empty after download attempt. Path: {index_path}")
            return []
        else:
            # Update timestamp for LRU cache eviction
            try:
                shard_dir.touch()
            except Exception:
                pass # Ignore errors here, not critical
            # print(f"Shard {shard_id:03d} index found. Size: {index_path.stat().st_size} bytes")

        # Load IDs mapping
        with open(ids_path, "r") as f:
            # ids.json maps "internal_id" -> {chunk_id, doc_id, text, ...}
            # Wait, let's check the format from INDEX.MD or shards.py
            # ids.json keeps the HNSW label -> chunk mapping
            # It seems to be a list of objects: [{"label": 0, ...}, {"label": 1, ...}]
            raw_data = json.load(f)
            if isinstance(raw_data, list):
                id_map = {str(item["label"]): item for item in raw_data}
            else:
                id_map = raw_data

        # Load HNSW index
        # We need to know dim and space. Usually cosine or l2.
        # Let's assume dim matches query_embedding
        dim = query_embedding.shape[0]
        p = hnswlib.Index(space='cosine', dim=dim)
        p.load_index(str(index_path))
        
        # Search
        labels, distances = p.knn_query(query_embedding, k=min(top_n, p.get_current_count()))
        
        results = []
        for label, dist in zip(labels[0], distances[0]):
            # Try both string and int lookup just in case
            label_str = str(label)
            label_int = int(label)
            
            data = None
            if label_str in id_map:
                data = id_map[label_str]
            elif label_int in id_map: # In case json loaded keys as ints (unlikely for json but possible if not standard json)
                data = id_map[label_int]
            else:
                # Debugging: Print first few keys to see what they look like
                first_keys = list(id_map.keys())[:5]
                print(f"Warning: Label {label} (type {type(label)}) not found in id_map. Keys sample: {first_keys}")

            if data:
                # data has chunk_text, chunk_id, doc_id, chunk_len
                results.append(SearchResult(
                    chunk_id=data.get("chunk_id", "unknown"),
                    doc_id=data.get("doc_id", "unknown"),
                    score=float(1 - dist), # Convert distance to similarity score roughly
                    text=data.get("chunk_text", ""),
                    metadata=data
                ))
                
        return results

    def search(self, query_embedding: np.ndarray, k_shards: int = 3, top_n_per_shard: int = 5) -> List[SearchResult]:
        shard_ids = self.find_nearest_shards(query_embedding, k=k_shards)
        print(f"Routing to shards: {shard_ids}")
        
        all_results = []
        for shard_id in shard_ids:
            results = self.search_shard(shard_id, query_embedding, top_n=top_n_per_shard)
            all_results.extend(results)
            
        # Sort by score (descending) and take top N
        all_results.sort(key=lambda x: x.score, reverse=True)
        return all_results[:top_n_per_shard] # Return overall top N
