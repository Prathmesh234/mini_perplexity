import io
import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

import numpy as np
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobClient, BlobServiceClient, ContainerClient
from dotenv import load_dotenv

DEFAULT_CLIENT_PREFIX = "vector-indexes-client1"
CENTROIDS_BLOB = "centroids.npy"
METADATA_BLOB = "metadata.json"
SHARDS_FOLDER = "shards"


def load_blob_config() -> Dict[str, str]:
    """
    Read connection info for Azure Blob Storage from .env or process env.
    """
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=False)

    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
    account_name = os.getenv("AZURE_STORAGE_ACCOUNT", "")
    container_name = os.getenv("AZURE_VECTOR_CONTAINER", "vectorindexes")

    if not connection_string:
        raise RuntimeError("AZURE_STORAGE_CONNECTION_STRING env var is required for blob uploads.")

    return {
        "connection_string": connection_string,
        "container_name": container_name,
        "account_name": account_name,
    }


def get_container_client() -> ContainerClient:
    """
    Return a container client, creating the container if it does not exist.
    """
    config = load_blob_config()
    service_client = BlobServiceClient.from_connection_string(config["connection_string"])
    container_client = service_client.get_container_client(config["container_name"])
    _ensure_container(container_client)
    return container_client


def _ensure_container(container_client: ContainerClient) -> None:
    try:
        container_client.create_container()
    except ResourceExistsError:
        pass


def _blob_path(client_prefix: str, *parts: str) -> str:
    # Example: client_prefix="vector-indexes-client1" and parts=("shards", "shard_001", "index.bin")
    # returns "vector-indexes-client1/shards/shard_001/index.bin"
    prefix = client_prefix.rstrip("/")
    extras = "/".join(part.strip("/") for part in parts if part)
    return f"{prefix}/{extras}" if extras else prefix


def _upload_bytes(blob_client: BlobClient, payload: bytes) -> str:
    blob_client.upload_blob(payload, overwrite=True)
    return blob_client.blob_name


def _np_to_bytes(array: np.ndarray) -> bytes:
    buffer = io.BytesIO()
    np.save(buffer, array)
    return buffer.getvalue()


def ensure_centroids_blob(
    container_client: ContainerClient,
    client_prefix: str = DEFAULT_CLIENT_PREFIX,
    centroids: Optional[np.ndarray] = None,
) -> str:
    """
    Ensure centroids.npy exists under the client prefix (upload provided centroids when missing).
    """
    blob_name = _blob_path(client_prefix, CENTROIDS_BLOB)
    blob_client = container_client.get_blob_client(blob=blob_name)

    if centroids is None and blob_client.exists():
        return blob_name

    array = np.asarray(centroids, dtype=np.float32) if centroids is not None else np.empty((0,), dtype=np.float32)
    payload = _np_to_bytes(array)
    _upload_bytes(blob_client, payload)
    return blob_name


def ensure_root_metadata_blob(
    container_client: ContainerClient,
    client_prefix: str = DEFAULT_CLIENT_PREFIX,
    metadata: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Ensure metadata.json exists (writes provided payload or empty dict).
    """
    blob_name = _blob_path(client_prefix, METADATA_BLOB)
    blob_client = container_client.get_blob_client(blob=blob_name)

    if metadata is None and blob_client.exists():
        return blob_name

    payload = json.dumps(metadata or {}, indent=2).encode("utf-8")
    _upload_bytes(blob_client, payload)
    return blob_name


def bootstrap_vector_index_storage(
    centroids: Optional[np.ndarray] = None,
    root_metadata: Optional[Dict[str, Any]] = None,
    client_prefix: str = DEFAULT_CLIENT_PREFIX,
) -> Dict[str, str]:
    """
    Ensure the base blobs exist (centroids.npy, metadata.json, shards/ prefix placeholder).
    """
    container_client = get_container_client()
    centroids_blob = ensure_centroids_blob(container_client, client_prefix, centroids=centroids)
    metadata_blob = ensure_root_metadata_blob(container_client, client_prefix, metadata=root_metadata)
    shards_blob = _blob_path(client_prefix, SHARDS_FOLDER, "init.txt")
    container_client.get_blob_client(shards_blob).upload_blob(b"", overwrite=True)

    return {
        "container": container_client.container_name,
        "centroids_blob": centroids_blob,
        "metadata_blob": metadata_blob,
        "shards_prefix": _blob_path(client_prefix, SHARDS_FOLDER),
    }


def upload_shard_artifacts(
    container_client: ContainerClient,
    shard_payload: Dict[str, Any],
    client_prefix: str = DEFAULT_CLIENT_PREFIX,
) -> Dict[str, str]:
    """
    Upload shard index/vectors/ids/meta blobs and return the blob names.
    """
    shard_id = shard_payload["shard_id"]
    artifacts = shard_payload["artifacts"]
    metadata = shard_payload["metadata"]

    shard_base = _blob_path(client_prefix, SHARDS_FOLDER, shard_id)
    names_map = {
        "index_bin": "index.bin",
        "vectors_npy": "vectors.npy",
        "ids_json": "ids.json",
    }

    uploaded_paths: Dict[str, str] = {}
    for key, filename in names_map.items():
        blob_name = _blob_path(shard_base, filename)
        blob_client = container_client.get_blob_client(blob=blob_name)
        _upload_bytes(blob_client, artifacts[key])
        uploaded_paths[key] = blob_name

    metadata_blob_name = _blob_path(shard_base, "meta.json")
    metadata_blob = container_client.get_blob_client(blob=metadata_blob_name)
    metadata_bytes = json.dumps(metadata, indent=2).encode("utf-8")
    _upload_bytes(metadata_blob, metadata_bytes)
    uploaded_paths["metadata_json"] = metadata_blob_name

    return uploaded_paths
