import gzip
import json
import re
from typing import List, Dict, Any, Generator
from azure.storage.blob import BlobServiceClient
import os
from pathlib import Path
from chunk_schema import Chunk
from service_bus import ServiceBusPublisher

class IndexerWorker:
    """Worker class to handle indexing of FineWeb files."""
    
    def __init__(self, worker_id: int, blob_connection_string: str, servicebus_connection_string: str = None):
        """
        Initialize the worker.
        
        Args:
            worker_id: Unique identifier for this worker
            blob_connection_string: Azure Blob Storage connection string
            servicebus_connection_string: Azure Service Bus connection string
        """
        self.worker_id = worker_id
        self.blob_service_client = BlobServiceClient.from_connection_string(blob_connection_string)
        self.container_name = "commoncrawl-wet"  # Corrected container name
        
        # Initialize Service Bus publisher
        self.service_bus_publisher = ServiceBusPublisher(servicebus_connection_string) if servicebus_connection_string else None
        
    def chunk_text_by_paragraphs(self, text: str, max_words: int = 300) -> List[str]:
        """
        Break text into paragraph-based chunks with max word count.
        
        Args:
            text: Input text to chunk
            max_words: Maximum words per chunk
            
        Returns:
            List of text chunks
        """
        # Split by double newlines to get paragraphs
        paragraphs = re.split(r'\n\s*\n', text.strip())
        
        chunks = []
        current_chunk = []
        current_word_count = 0
        
        for paragraph in paragraphs:
            paragraph = paragraph.strip()
            if not paragraph:
                continue
                
            word_count = len(paragraph.split())
            
            # If adding this paragraph exceeds limit, finalize current chunk
            if current_word_count + word_count > max_words and current_chunk:
                chunks.append('\n\n'.join(current_chunk))
                current_chunk = [paragraph]
                current_word_count = word_count
            else:
                current_chunk.append(paragraph)
                current_word_count += word_count
        
        # Add remaining chunk if any
        if current_chunk:
            chunks.append('\n\n'.join(current_chunk))
            
        return chunks
    
    def process_jsonl_file(self, file_path: str) -> Generator[Chunk, None, None]:
        """
        Process a single JSONL.gz file and yield chunks.
        
        Args:
            file_path: Path to the JSONL.gz file in blob storage
            
        Yields:
            Chunk objects
        """
        try:
            # Download blob content
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name, 
                blob=file_path
            )
            
            print(f"Worker {self.worker_id}: Processing {file_path}")
            
            # Download and decompress
            blob_data = blob_client.download_blob().readall()
            
            # Decompress the gzipped data
            decompressed_data = gzip.decompress(blob_data)
            content = decompressed_data.decode('utf-8')
             # Process each line in JSONL
            for line_num, line in enumerate(content.strip().split('\n')):
                if not line.strip():
                    continue
                    
                try:
                    # Parse JSON line
                    doc = json.loads(line)
                    doc_text = doc.get('text', '')
                    doc_id = doc.get('id', f"{file_path}_{line_num}")
                    
                    if not doc_text:
                        continue
                        
                    # Break into chunks
                    text_chunks = self.chunk_text_by_paragraphs(doc_text)
                    
                    # Yield each chunk as Chunk object
                    for chunk_idx, chunk_text in enumerate(text_chunks):
                        chunk = Chunk(
                            chunk=chunk_text,
                            doc_id=doc_id,
                            chunk_len=len(chunk_text),
                            chunk_id=f"{doc_id}_chunk_{chunk_idx}",
                            source_file=file_path
                        )
                        yield chunk
                        
                except json.JSONDecodeError as e:
                    print(f"Worker {self.worker_id}: JSON decode error in {file_path} line {line_num}: {e}")
                    continue
                        
        except Exception as e:
            print(f"Worker {self.worker_id}: Error processing {file_path}: {e}")
            
    def process_file_range(self, start_file: int, end_file: int, send_to_servicebus: bool = True) -> List[Chunk]:
        """
        Process a range of FineWeb files.
        
        Args:
            start_file: Starting file number (inclusive)
            end_file: Ending file number (exclusive)
            send_to_servicebus: Whether to send chunks to Service Bus
            
        Returns:
            List of all chunks processed
        """
        all_chunks = []
        
        for file_num in range(start_file, end_file):
            file_path = f"fineweb/train/fineweb-train-{file_num:05d}.jsonl.gz"
            
            try:
                for chunk in self.process_jsonl_file(file_path):
                    all_chunks.append(chunk)
                    
                    # Send chunk to Service Bus if enabled
                    if send_to_servicebus and self.service_bus_publisher:
                        self.service_bus_publisher.send_chunk(chunk)
                    
            except Exception as e:
                print(f"Worker {self.worker_id}: Failed to process file {file_path}: {e}")
                continue
                
        print(f"Worker {self.worker_id}: Completed processing files {start_file}-{end_file-1}, generated {len(all_chunks)} chunks")
        return all_chunks
