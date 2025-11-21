from fastapi import FastAPI, HTTPException

from centroids import create_centroids as create_centroids_task
from shards import create_shards
from store_blob import (
    get_container_client,
    ensure_centroids_blob,
    ensure_root_metadata_blob,
    upload_shard_artifacts,
)

app = FastAPI(title="Insert Index Service", version="0.1.0")


@app.post("/create-hnsw")
def create_hnsw(
    max_messages: int = 3000,
    num_centroids: int = 30,
    shard_prefix: str = "shard_",
    client_prefix: str = "vector-indexes-client1",
):
    """
    Fetch embeddings, generate centroids, build HNSW shards, and upload artifacts to blob storage.
    """
    try:
        result = create_centroids_task(
            max_messages=max_messages,
            num_centroids=num_centroids,
        )
    except ValueError:
        return {"status": "empty"}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    centroid_matrix = result.pop("centroids")
    chunks = result.pop("chunks")

    shards = create_shards(
        chunks=chunks,
        centroids=centroid_matrix,
        shard_prefix=shard_prefix,
        blob_base_path=client_prefix,
    )

    container_client = get_container_client()
    centroids_blob = ensure_centroids_blob(container_client, client_prefix, centroid_matrix)
    metadata_blob = ensure_root_metadata_blob(container_client, client_prefix, metadata=result["metadata"])

    shard_uploads = [
        upload_shard_artifacts(container_client, shard_payload, client_prefix=client_prefix)
        for shard_payload in shards
    ]

    response_payload = {
        "status": "completed",
        "container": container_client.container_name,
        "centroids_blob": centroids_blob,
        "metadata_blob": metadata_blob,
        "num_shards": len(shard_uploads),
        "shards": shard_uploads,
        "centroids_path": result.get("centroids_path"),
        "metadata_path": result.get("metadata_path"),
    }

    return response_payload


@app.get("/health")
def health():
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8001)
