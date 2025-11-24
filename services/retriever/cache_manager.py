import os
import time
import shutil
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - CACHE_MANAGER - %(message)s')
logger = logging.getLogger(__name__)

CACHE_DIR = Path("/tmp/retriever_cache")
MAX_CACHE_SIZE_GB = 2.0  # Limit to 2GB
CHECK_INTERVAL_SECONDS = 60

def get_dir_size(path: Path) -> int:
    """Calculate total size of a directory in bytes."""
    total = 0
    try:
        for entry in os.scandir(path):
            if entry.is_file():
                total += entry.stat().st_size
            elif entry.is_dir():
                total += get_dir_size(Path(entry.path))
    except FileNotFoundError:
        pass
    return total

def cleanup_cache():
    """Remove least recently used shards if cache size exceeds limit."""
    if not CACHE_DIR.exists():
        return

    # Calculate current size
    current_size = get_dir_size(CACHE_DIR)
    current_size_gb = current_size / (1024**3)
    
    if current_size_gb < MAX_CACHE_SIZE_GB:
        # logger.info(f"Cache size: {current_size_gb:.2f} GB (Under limit of {MAX_CACHE_SIZE_GB} GB)")
        return

    logger.info(f"Cache size {current_size_gb:.2f} GB exceeds limit {MAX_CACHE_SIZE_GB} GB. Starting cleanup...")

    # List all shard directories
    shards = []
    for item in CACHE_DIR.iterdir():
        if item.is_dir() and item.name.startswith("shard_"):
            try:
                # Use modification time (mtime) as "last used" time
                # retrieval.py updates this with touch() on access
                mtime = item.stat().st_mtime
                shards.append((item, mtime))
            except OSError:
                pass

    # Sort by mtime (oldest first)
    shards.sort(key=lambda x: x[1])

    bytes_to_free = current_size - (MAX_CACHE_SIZE_GB * 1024**3)
    freed_bytes = 0

    for shard_dir, mtime in shards:
        if freed_bytes >= bytes_to_free:
            break
        
        try:
            shard_size = get_dir_size(shard_dir)
            logger.info(f"Removing old shard: {shard_dir.name} (Last used: {time.ctime(mtime)})")
            shutil.rmtree(shard_dir)
            freed_bytes += shard_size
        except Exception as e:
            logger.error(f"Error removing {shard_dir.name}: {e}")

    logger.info(f"Cleanup finished. Freed {freed_bytes / (1024**2):.2f} MB.")

def main():
    logger.info("Starting Cache Manager...")
    logger.info(f"Monitoring {CACHE_DIR}")
    logger.info(f"Max Size: {MAX_CACHE_SIZE_GB} GB")
    
    while True:
        try:
            cleanup_cache()
        except Exception as e:
            logger.error(f"Error in cleanup loop: {e}")
        
        time.sleep(CHECK_INTERVAL_SECONDS)

if __name__ == "__main__":
    main()
