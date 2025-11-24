import os
from pathlib import Path
from typing import Optional, Dict
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

def load_env_config():
    """Load environment variables from .env file."""
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=False)
    
    config = {
        'conn_str': os.getenv("AZURE_STORAGE_CONNECTION_STRING"),
        'container': os.getenv("AZURE_VECTOR_CONTAINER", "vectorindexes"),
        'blob_prefix': os.getenv("AZURE_BLOB_PREFIX", "vector-indexes-client1"),
    }
    
    if not config['conn_str']:
        raise RuntimeError("AZURE_STORAGE_CONNECTION_STRING env var is required")
        
    print(f"DEBUG: Loaded config: container='{config['container']}', prefix='{config['blob_prefix']}'")
    return config

def get_container_client():
    config = load_env_config()
    blob_service_client = BlobServiceClient.from_connection_string(config['conn_str'])
    return blob_service_client.get_container_client(config['container'])

def download_blob_to_file(blob_name: str, local_path: Path) -> Path:
    """Download a blob to a local file."""
    config = load_env_config()
    
    # Prepend prefix if it exists and isn't already in the blob name
    prefix = config['blob_prefix']
    if prefix and not blob_name.startswith(prefix):
        full_blob_name = f"{prefix}/{blob_name}"
    else:
        full_blob_name = blob_name
        
    container_client = get_container_client()
    blob_client = container_client.get_blob_client(full_blob_name)
    
    if not blob_client.exists():
        # Try without prefix just in case
        blob_client = container_client.get_blob_client(blob_name)
        if not blob_client.exists():
            raise FileNotFoundError(f"Blob {full_blob_name} (or {blob_name}) not found in container {config['container']}")
        
    local_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(local_path, "wb") as f:
        download_stream = blob_client.download_blob()
        f.write(download_stream.readall())
        
    return local_path

def download_centroids(local_dir: Path) -> Path:
    """Download centroids.npy to local directory."""
    return download_blob_to_file("centroids.npy", local_dir / "centroids.npy")

def download_shard_artifacts(shard_id: int, local_dir: Path) -> Dict[str, Path]:
    """Download all artifacts for a specific shard."""
    shard_prefix = f"shards/shard_{shard_id:03d}"
    artifacts = {}
    
    for filename in ["index.bin", "ids.json", "vectors.npy", "meta.json"]:
        blob_name = f"{shard_prefix}/{filename}"
        local_file = local_dir / f"shard_{shard_id:03d}" / filename
        try:
            download_blob_to_file(blob_name, local_file)
            artifacts[filename] = local_file
        except FileNotFoundError:
            print(f"Warning: {blob_name} not found")
            
    return artifacts
