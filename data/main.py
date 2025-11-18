import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

from fineweb import stream_fineweb_to_azure


def _str_to_int(value: Optional[str], default: int) -> int:
    if value is None or value.strip() == "":
        return default
    try:
        return int(value)
    except ValueError:
        return default


def main():
    # Auto-load .env from the data directory
    env_path = Path(__file__).with_name(".env")
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=False)

    azure_conn_str = os.getenv("AZURE_CONN_STR") or os.getenv(
        "AZURE_STORAGE_CONNECTION_STRING", ""
    )
    if not azure_conn_str:
        raise RuntimeError("AZURE_CONN_STR env var is required")

    container_name = os.getenv("CONTAINER_NAME", "fineweb-raw")
    dataset_id = os.getenv("DATASET_ID", "HuggingFaceFW/fineweb")
    dataset_split = os.getenv("DATASET_SPLIT", "train")
    chunk_size = _str_to_int(os.getenv("CHUNK_SIZE"), 10_000)
    blob_prefix = os.getenv("BLOB_PREFIX", f"fineweb/{dataset_split}")
    max_chunks_value = _str_to_int(os.getenv("MAX_CHUNKS"), 0)
    max_chunks = max_chunks_value if max_chunks_value > 0 else None
    upload_retries = _str_to_int(os.getenv("UPLOAD_RETRIES"), 3)

    summary = stream_fineweb_to_azure(
        azure_connection_string=azure_conn_str,
        container_name=container_name,
        dataset_id=dataset_id,
        split=dataset_split,
        chunk_size=chunk_size,
        blob_prefix=blob_prefix,
        max_chunks=max_chunks,
        upload_retries=max(upload_retries, 1),
    )
    print(summary)


if __name__ == "__main__":
    main()
