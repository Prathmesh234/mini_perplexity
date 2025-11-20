from fastapi import FastAPI, HTTPException

from centroids import create_centroids as create_centroids_task

app = FastAPI(title="Insert Index Service", version="0.1.0")


@app.post("/create-centroids")
def create_centroids(
    max_messages: int = 3000,
    subscription_name: str = "ingestion-sub",
    num_centroids: int = 30,
):
    """
    Poll Service Bus in batches and run centroid creation once samples are available.
    """
    while True:
        try:
            result = create_centroids_task(
                subscription_name=subscription_name,
                max_messages=max_messages,
                num_centroids=num_centroids,
            )
            return {"status": "completed", **result}
        except ValueError:
            # No embeddings available yet; stop looping for now to avoid tight polling
            break
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc))

    # Reached here because Service Bus had no messages in this polling loop
    return {"status": "empty"}


@app.post("/create-hnsw")
def create_hnsw():
    """Placeholder for future HNSW construction endpoint."""
    return {"status": "not_implemented"}


@app.get("/health")
def health():
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8001)
