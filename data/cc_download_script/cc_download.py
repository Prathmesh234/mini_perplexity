import io
import gzip
import random
from typing import Optional, Dict
import requests
from tqdm import tqdm
from azure.storage.blob import BlobServiceClient

from cc_download_script.filter import is_english_wet_file


def download_cc_wet_to_azure(
    azure_connection_string: str,
    container_name: str,
    index_url: str = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-43/wet.paths.gz",
    target_mb: int = 5 * 1024,
    avg_file_mb: int = 100,
    seed: Optional[int] = None,
    request_timeout_s: int = 60,
    min_english_files: int = 0,
) -> Dict[str, int]:
    """
    Download a subset of Common Crawl WET files and upload them to Azure Blob Storage.

    - Chooses approximately target_mb total by sampling paths with an assumed avg_file_mb per file.
    - Skips blobs that already exist.
    - Optionally keeps downloading until min_english_files are stored.

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

    if min_english_files > 0:
        subset = paths[:]
        rng.shuffle(subset)
        attempt_limit = len(subset)
    else:
        subset = rng.sample(paths, files_to_download)
        attempt_limit = len(subset)

    downloaded = 0
    skipped_existing = 0
    skipped_language = 0
    attempted = 0

    for path in tqdm(subset, desc="Uploading to Azure", total=attempt_limit):
        if min_english_files > 0 and downloaded >= min_english_files:
            break
        attempted += 1
        file_url = f"https://data.commoncrawl.org/{path}"
        blob_name = path.split("/")[-1]

        blob_client = container.get_blob_client(blob=blob_name)
        if blob_client.exists():
            skipped_existing += 1
            continue

        with requests.get(file_url, stream=True, timeout=max(60, request_timeout_s * 5)) as r:
            r.raise_for_status()
            payload = r.content

        if not is_english_wet_file(payload):
            skipped_language += 1
            continue

        blob_client.upload_blob(io.BytesIO(payload), overwrite=True)
        downloaded += 1

    return {
        "downloaded": downloaded,
        "skipped_existing": skipped_existing,
        "skipped_language": skipped_language,
        "attempted": attempted,
    }
