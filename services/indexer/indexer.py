import multiprocessing
import os
from typing import List, Dict, Any
from concurrent.futures import ProcessPoolExecutor, as_completed
from worker import IndexerWorker
from chunk_schema import Chunk
from dotenv import load_dotenv

class FineWebIndexer:
    """Main indexer class for processing FineWeb data with parallel workers."""
    
    def __init__(self, blob_connection_string: str = None, servicebus_connection_string: str = None):
        """
        Initialize the indexer.
        
        Args:
            blob_connection_string: Azure Blob Storage connection string
            servicebus_connection_string: Azure Service Bus connection string
        """
        # Load environment variables
        load_dotenv()
        
        self.blob_connection_string = blob_connection_string or os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        self.servicebus_connection_string = servicebus_connection_string or os.getenv("AZURE_SERVICE_BUS_CONNECTION_STRING")
        
        if not self.blob_connection_string:
            raise ValueError("Azure Storage connection string is required")
            
        self.num_workers = 4
        self.total_files = 205  # fineweb-train-00000.jsonl.gz to fineweb-train-00204.jsonl.gz
        self.files_per_worker = 51  # 204 files / 4 workers = 51 files per worker
        
    def calculate_file_ranges(self) -> List[tuple]:
        """
        Calculate file ranges for each worker to ensure no overlap.
        
        Returns:
            List of (start_file, end_file) tuples for each worker
        """
        ranges = []
        
        for worker_id in range(self.num_workers):
            start_file = worker_id * self.files_per_worker
            end_file = min(start_file + self.files_per_worker, self.total_files)
            ranges.append((start_file, end_file))
            
        return ranges
        
    def process_worker_range(self, worker_data: tuple) -> List[Chunk]:
        """
        Process files for a single worker.
        
        Args:
            worker_data: Tuple of (worker_id, start_file, end_file)
            
        Returns:
            List of chunks processed by this worker
        """
        worker_id, start_file, end_file = worker_data
        
        print(f"Starting worker {worker_id} for files {start_file}-{end_file-1}")
        
        # Create worker instance
        worker = IndexerWorker(worker_id, self.blob_connection_string, self.servicebus_connection_string)
        
        # Process the assigned file range
        chunks = worker.process_file_range(start_file, end_file, send_to_servicebus=True)
        
        print(f"Worker {worker_id} completed: {len(chunks)} chunks")
        return chunks
        
    def run_indexing(self) -> List[Chunk]:
        """
        Run the indexing process with parallel workers.
        
        Returns:
            List of all chunks from all workers
        """
        print(f"Starting FineWeb indexing with {self.num_workers} workers")
        print(f"Processing {self.total_files} files total")
        print(f"Service Bus enabled: {self.servicebus_connection_string is not None}")
        
        # Calculate file ranges for each worker
        file_ranges = self.calculate_file_ranges()
        
        # Prepare worker data
        worker_data = [
            (worker_id, start_file, end_file)
            for worker_id, (start_file, end_file) in enumerate(file_ranges)
        ]
        
        print("File distribution:")
        for worker_id, (start_file, end_file) in enumerate(file_ranges):
            print(f"  Worker {worker_id}: files {start_file}-{end_file-1} ({end_file-start_file} files)")
        
        all_chunks = []
        
        # Use ProcessPoolExecutor for parallel processing
        with ProcessPoolExecutor(max_workers=self.num_workers) as executor:
            # Submit all worker tasks
            future_to_worker = {
                executor.submit(self.process_worker_range, data): data[0]
                for data in worker_data
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_worker):
                worker_id = future_to_worker[future]
                try:
                    worker_chunks = future.result()
                    all_chunks.extend(worker_chunks)
                    print(f"Collected {len(worker_chunks)} chunks from worker {worker_id}")
                    
                except Exception as e:
                    print(f"Worker {worker_id} failed with error: {e}")
                    
        print(f"\nIndexing completed!")
        print(f"Total chunks generated: {len(all_chunks)}")
        
        return all_chunks

# Convenience function for external use
def run_fineweb_indexing(blob_connection_string: str = None, servicebus_connection_string: str = None) -> List[Chunk]:
    """
    Run FineWeb indexing process.
    
    Args:
        blob_connection_string: Azure Blob Storage connection string
        servicebus_connection_string: Azure Service Bus connection string
        
    Returns:
        List of all processed chunks
    """
    indexer = FineWebIndexer(blob_connection_string, servicebus_connection_string)
    return indexer.run_indexing()
