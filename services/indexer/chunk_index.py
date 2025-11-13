import os
import gzip
import uuid
from pathlib import Path
from typing import List, Iterator
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from tqdm import tqdm


class Chunk:
    def __init__(self, chunk: str, id: str, doc_id: str):
        self.chunk = chunk
        self.id = id
        self.doc_id = doc_id


def _parse_wet_content(content: bytes) -> str:
    """
    Parse WET file content to extract text.
    WET files are gzipped WARC files. This extracts the text content from WARC records.
    """
    try:
        # Decompress gzip
        decompressed = gzip.decompress(content)
        text = decompressed.decode('utf-8', errors='ignore')
        
        # Simple WARC parsing: extract text content, skip WARC headers
        lines = text.split('\n')
        extracted_text = []
        skip_until_blank = False
        
        for line in lines:
            # Skip WARC header lines
            if line.startswith('WARC/'):
                skip_until_blank = True
                continue
            elif line.startswith('WARC-'):
                continue
            elif line.strip() == '':
                skip_until_blank = False
                continue
            elif not skip_until_blank:
                extracted_text.append(line)
        
        return '\n'.join(extracted_text)
    except Exception:
        # If parsing fails, try to decode directly
        try:
            return content.decode('utf-8', errors='ignore')
        except Exception:
            return ""


def _split_into_chunks(text: str, chunk_size: int = 1500, overlap: int = 200) -> List[str]:
    """
    Split text into chunks of approximately chunk_size characters with overlap.
    Optimal chunk size for embeddings: 1500 characters (~375-500 tokens).
    This balances context preservation with processing efficiency.
    """
    if not text:
        return []
    
    # If text is smaller than chunk size, return as single chunk
    if len(text) <= chunk_size:
        return [text.strip()] if text.strip() else []
    
    chunks = []
    start = 0
    min_chunk_size = chunk_size * 0.5  # Minimum chunk size to avoid tiny chunks
    
    while start < len(text):
        end = start + chunk_size
        chunk = text[start:end]
        
        # Try to break at natural boundaries (sentence or paragraph endings)
        if end < len(text):
            # Priority order: paragraph breaks, then sentence endings
            for sep in ['\n\n', '\n', '. ', '.\n', '! ', '!\n', '? ', '?\n', '; ', ';']:
                last_sep = chunk.rfind(sep)
                if last_sep >= min_chunk_size:  # Only break if we're past minimum size
                    chunk = chunk[:last_sep + len(sep)]
                    end = start + last_sep + len(sep)
                    break
        
        chunk_text = chunk.strip()
        # Only add non-empty chunks that meet minimum size
        if chunk_text and len(chunk_text) >= min_chunk_size:
            chunks.append(chunk_text)
        
        # Move start position with overlap, but ensure we make progress
        new_start = end - overlap
        if new_start <= start:
            new_start = start + 1  # Ensure we always advance
        start = new_start
        
        # Handle remaining text that's smaller than chunk_size
        if start >= len(text):
            break
    
    # Add any remaining text as final chunk if it's substantial
    if start < len(text):
        remaining = text[start:].strip()
        if remaining and len(remaining) >= min_chunk_size:
            chunks.append(remaining)
        elif remaining and chunks:  # If small but exists, merge with last chunk
            chunks[-1] = chunks[-1] + " " + remaining
    
    return chunks


def chunk_index(
    azure_connection_string: str = None,
    container_name: str = None,
    chunk_size: int = 1500,
    overlap: int = 200,
) -> Iterator[Chunk]:
    """
    Read files from Azure Blob Storage, parse them, and yield Chunk objects.
    
    Args:
        azure_connection_string: Azure Storage connection string. If None, loads from .env
        container_name: Container name. If None, loads from .env (default: "commoncrawl-wet")
        chunk_size: Approximate size of each chunk in characters (default: 1500, optimal for embeddings)
        overlap: Overlap between chunks in characters (default: 200, ~13% overlap for context retention)
    
    Yields:
        Chunk objects with chunk text, unique id, and doc_id
    """
    # Load environment variables if not provided
    # Try local .env first, then fall back to data/.env
    local_env_path = Path(__file__).parent / ".env"
    if local_env_path.exists():
        load_dotenv(dotenv_path=local_env_path, override=False)
    else:
        data_env_path = Path(__file__).parent.parent.parent / "data" / ".env"
        if data_env_path.exists():
            load_dotenv(dotenv_path=data_env_path, override=False)
    
    if azure_connection_string is None:
        azure_connection_string = os.getenv("AZURE_CONN_STR", "")
        if not azure_connection_string:
            raise RuntimeError("AZURE_CONN_STR env var is required")
    
    if container_name is None:
        container_name = os.getenv("CONTAINER_NAME", "commoncrawl-wet")
    
    # Connect to Azure Blob Storage
    blob_service = BlobServiceClient.from_connection_string(azure_connection_string)
    container = blob_service.get_container_client(container_name)
    
    # Iterate through all blobs in the container
    blob_list = container.list_blobs()
    blob_iterator = tqdm(
        blob_list,
        desc="Processing documents",
        unit="doc",
        leave=False,
    )

    for blob in blob_iterator:
        blob_name = blob.name
        blob_client = container.get_blob_client(blob=blob_name)
        
        # Download blob content
        blob_data = blob_client.download_blob().readall()
        
        # Parse WET content
        text_content = _parse_wet_content(blob_data)
        
        # Split into chunks
        text_chunks = _split_into_chunks(text_content, chunk_size=chunk_size, overlap=overlap)
        
        # Create Chunk objects
        for chunk_text in text_chunks:
            if chunk_text.strip():  # Only yield non-empty chunks
                chunk_id = str(uuid.uuid4())
                chunk_obj = Chunk(
                    chunk=chunk_text,
                    id=chunk_id,
                    doc_id=blob_name
                )
                yield chunk_obj
