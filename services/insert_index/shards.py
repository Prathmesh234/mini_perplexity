import io
import json
import os
import tempfile
from datetime import datetime, timezone
from typing import Any, Dict, List, Sequence

import hnswlib
import numpy as np

from schema import EmbeddingChunk


def _serialize_index(index: hnswlib.Index) -> bytes:
    """
    hnswlib only supports saving to disk, so we use a temporary file and capture the bytes.
    """
    with tempfile.NamedTemporaryFile(suffix=".bin", delete=False) as tmp:
        temp_path = tmp.name

    try:
        index.save_index(temp_path)
        with open(temp_path, "rb") as fh:
            return fh.read()
    finally:
        try:
            os.remove(temp_path)
        except OSError:
            pass


def _serialize_vectors(array: np.ndarray) -> bytes:
    buffer = io.BytesIO()
    np.save(buffer, array)
    return buffer.getvalue()


def _serialize_json(payload: Any) -> bytes:
    return json.dumps(payload, indent=2).encode("utf-8")


def create_shards(
    chunks: Sequence[EmbeddingChunk],
    centroids: np.ndarray,
    shard_prefix: str = "shard_",
    version: str = "v1",
    blob_base_path: str = "vector-indexes",
) -> List[Dict[str, Any]]:
    """
    Assign embedding chunks to the nearest centroid and create an HNSW index per shard.

    Returns a list of dictionaries, each containing the shard metadata plus in-memory
    artifacts (`index_bin`, `vectors_npy`, `ids_json`) ready for blob uploads.
    """
    if not chunks:
        return []

    centroid_matrix = np.asarray(centroids, dtype=np.float32)
    if centroid_matrix.ndim != 2:
        raise ValueError("Centroids must be a 2D array.")
    if centroid_matrix.size == 0:
        raise ValueError("No centroid vectors provided.")

    vectors = np.asarray([chunk.embedding for chunk in chunks], dtype=np.float32)
    if vectors.ndim != 2 or vectors.shape[1] != centroid_matrix.shape[1]:
        raise ValueError("Embedding dimensions do not match centroid dimensions.")

    # Compute nearest centroid for every embedding
    distances = np.linalg.norm(
        vectors[:, None, :] - centroid_matrix[None, :, :], axis=2
    )
    assignments = distances.argmin(axis=1)

    hnsw_params = {
        "space": "l2",
        "M": 64,
        "ef_construction": 200,
        "ef_runtime": 64,
    }
    created_at = datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")

    shard_metadata: List[Dict[str, Any]] = []
    num_centroids = centroid_matrix.shape[0]

    for centroid_idx in range(num_centroids):
        shard_indices = np.where(assignments == centroid_idx)[0]
        if shard_indices.size == 0:
            continue

        shard_id = f"{shard_prefix}{centroid_idx:03d}"

        shard_vectors = vectors[shard_indices]
        shard_labels = np.arange(len(shard_vectors), dtype=np.int64)

        index = hnswlib.Index(space=hnsw_params["space"], dim=shard_vectors.shape[1])
        index.init_index(
            max_elements=len(shard_vectors),
            ef_construction=hnsw_params["ef_construction"],
            M=hnsw_params["M"],
        )
        index.add_items(shard_vectors, shard_labels)
        index.set_ef(hnsw_params["ef_runtime"])

        index_bytes = _serialize_index(index)
        vectors_bytes = _serialize_vectors(shard_vectors)

        ids_payload = []
        for local_label, source_idx in enumerate(shard_indices):
            chunk = chunks[int(source_idx)]
            ids_payload.append(
                {
                    "label": local_label,
                    "chunk_id": str(chunk.chunk_id),
                    "doc_id": str(chunk.doc_id),
                    "chunk_len": chunk.chunk_len,
                    "chunk_text": chunk.chunk,
                }
            )
        ids_bytes = _serialize_json(ids_payload)

        shard_blob_prefix = f"{blob_base_path}/shards/{shard_id}"

        metadata = {
            "shard_id": shard_id,
            "centroid_index": centroid_idx,
            "centroid_vector": centroid_matrix[centroid_idx].tolist(),
            "embedding_dim": shard_vectors.shape[1],
            "num_vectors": len(shard_vectors),
            "hnsw_params": hnsw_params,
            "blob_paths": {
                "index_bin": f"{shard_blob_prefix}/index.bin",
                "vectors_npy": f"{shard_blob_prefix}/vectors.npy",
                "ids_json": f"{shard_blob_prefix}/ids.json",
            },
            "created_at": created_at,
            "version": version,
        }
        shard_metadata.append(
            {
                "shard_id": shard_id,
                "metadata": metadata,
                "artifacts": {
                    "index_bin": index_bytes,
                    "vectors_npy": vectors_bytes,
                    "ids_json": ids_bytes,
                },
            }
        )

    return shard_metadata
