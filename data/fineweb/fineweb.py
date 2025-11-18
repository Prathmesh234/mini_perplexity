"""Utilities to stream HuggingFace FineWeb shards to Azure Blob Storage."""

from __future__ import annotations

import gzip
import io
import json
import time
from typing import Dict, List, Optional

from azure.core.exceptions import ResourceExistsError, ServiceResponseError
from azure.storage.blob import BlobServiceClient
from datasets import load_dataset


def _serialize_chunk_to_gz(records: List[dict]) -> bytes:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb") as gzip_file:
        for record in records:
            line = json.dumps(record, ensure_ascii=False)
            gzip_file.write(line.encode("utf-8"))
            gzip_file.write(b"\n")
    buffer.seek(0)
    return buffer.read()


def _upload_chunk(
    container_client,
    chunk_data: bytes,
    blob_prefix: str,
    split: str,
    shard_id: int,
    max_retries: int,
) -> str:
    blob_prefix = blob_prefix.strip("/")
    blob_name = f"{blob_prefix}/fineweb-{split}-{shard_id:05d}.jsonl.gz"
    max_retries = max(1, max_retries)
    for attempt in range(1, max_retries + 1):
        try:
            container_client.upload_blob(name=blob_name, data=chunk_data, overwrite=True)
            break
        except ServiceResponseError as exc:
            if attempt >= max_retries:
                raise
            delay = min(5 * attempt, 30)
            print(
                f"Upload failed for {blob_name} (attempt {attempt}/{max_retries}): {exc}. "
                f"Retrying in {delay}s."
            )
            time.sleep(delay)
    return blob_name


def stream_fineweb_to_azure(
    azure_connection_string: str,
    container_name: str,
    dataset_id: str = "HuggingFaceFW/fineweb",
    split: str = "train",
    chunk_size: int = 10_000,
    blob_prefix: str = "fineweb/train",
    max_chunks: Optional[int] = None,
    upload_retries: int = 3,
) -> Dict[str, int]:
    """Stream HuggingFace FineWeb dataset shards directly into Azure Blob Storage."""

    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than zero")
    if max_chunks is not None and max_chunks <= 0:
        raise ValueError("max_chunks must be positive when provided")

    blob_service = BlobServiceClient.from_connection_string(azure_connection_string)
    container_client = blob_service.get_container_client(container_name)
    try:
        container_client.create_container()
    except ResourceExistsError:
        pass

    dataset = load_dataset(dataset_id, split=split, streaming=True)

    buffer: List[dict] = []
    shard_id = 0
    documents_written = 0
    bytes_uploaded = 0

    for sample in dataset:
        buffer.append(sample)
        if len(buffer) < chunk_size:
            continue

        chunk_bytes = _serialize_chunk_to_gz(buffer)
        blob_name = _upload_chunk(
            container_client=container_client,
            chunk_data=chunk_bytes,
            blob_prefix=blob_prefix,
            split=split,
            shard_id=shard_id,
            max_retries=upload_retries,
        )
        bytes_uploaded += len(chunk_bytes)
        documents_written += len(buffer)
        print(
            f"Uploaded shard {shard_id} to {blob_name} with {len(buffer)} records ({len(chunk_bytes)} bytes)."
        )
        buffer.clear()
        shard_id += 1

        if max_chunks is not None and shard_id >= max_chunks:
            break

    if buffer and (max_chunks is None or shard_id < max_chunks):
        chunk_bytes = _serialize_chunk_to_gz(buffer)
        blob_name = _upload_chunk(
            container_client=container_client,
            chunk_data=chunk_bytes,
            blob_prefix=blob_prefix,
            split=split,
            shard_id=shard_id,
            max_retries=upload_retries,
        )
        bytes_uploaded += len(chunk_bytes)
        documents_written += len(buffer)
        print(
            f"Uploaded final shard {shard_id} to {blob_name} with {len(buffer)} records ({len(chunk_bytes)} bytes)."
        )
        shard_id += 1

    return {
        "dataset_id": dataset_id,
        "split": split,
        "chunks_uploaded": shard_id,
        "documents_uploaded": documents_written,
        "bytes_uploaded": bytes_uploaded,
    }
