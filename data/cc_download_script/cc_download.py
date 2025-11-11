import io
import gzip
import random
from typing import Optional, Dict
import requests
from tqdm import tqdm
from azure.storage.blob import BlobServiceClient


def download_cc_wet_to_azure(
    azure_connection_string: str,
    container_name: str,
    index_url: str = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-43/wet.paths.gz",
    target_mb: int = 5 * 1024,
    avg_file_mb: int = 100,
    seed: Optional[int] = None,
    request_timeout_s: int = 60,
) -> Dict[str, int]:
    """
    Download a subset of Common Crawl WET files and upload them to Azure Blob Storage.

    - Chooses approximately target_mb total by sampling paths with an assumed avg_file_mb per file.
    - Skips blobs that already exist.

    Returns a summary dict with counts: downloaded, skipped_existing, attempted.
    """
    if avg_file_mb <= 0:
        raise ValueError("avg_file_mb must be > 0")
    if target_mb <= 0:
        raise ValueError("target_mb must be > 0")

    blob_service = BlobServiceClient.from_connection_string(azure_connection_string)
    container = blob_service.get_container_client(container_name)
    try:
        container.create_container()
    except Exception:
        # Container may already exist
        pass

    resp = requests.get(index_url, timeout=request_timeout_s)
    resp.raise_for_status()
    paths = gzip.decompress(resp.content).decode().splitlines()
    if not paths:
        return {"downloaded": 0, "skipped_existing": 0, "attempted": 0}

    files_to_download = max(1, int(target_mb / max(1, avg_file_mb)))
    files_to_download = min(files_to_download, len(paths))

    rng = random.Random(seed) if seed is not None else random
    subset = rng.sample(paths, files_to_download)

    downloaded = 0
    skipped_existing = 0

    for path in tqdm(subset, desc="Uploading to Azure"):
        file_url = f"https://data.commoncrawl.org/{path}"
        blob_name = path.split("/")[-1]

        blob_client = container.get_blob_client(blob=blob_name)
        if blob_client.exists():
            skipped_existing += 1
            continue

        with requests.get(file_url, stream=True, timeout=max(60, request_timeout_s * 5)) as r:
            r.raise_for_status()
            blob_client.upload_blob(r.raw, overwrite=True)
            downloaded += 1

    return {"downloaded": downloaded, "skipped_existing": skipped_existing, "attempted": files_to_download}