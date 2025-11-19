from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import asyncio
import threading
import time
from indexer import run_fineweb_indexing
from datetime import datetime

# Initialize FastAPI app
app = FastAPI(title="FineWeb Indexer Service", version="1.0.0")

# Global state tracking
indexing_state = {
    "is_running": False,
    "start_time": None,
    "end_time": None,
    "total_chunks": 0,
    "current_status": "idle",
    "error_message": None
}

class IndexingStatus(BaseModel):
    is_running: bool
    status: str
    start_time: Optional[str]
    end_time: Optional[str]
    total_chunks: int
    duration_seconds: Optional[float]
    error_message: Optional[str]

class IndexingResult(BaseModel):
    success: bool
    message: str
    total_chunks: int
    duration_seconds: float

def run_indexing_task():
    """Background task to run the indexing process."""
    global indexing_state
    
    try:
        indexing_state.update({
            "is_running": True,
            "start_time": datetime.now().isoformat(),
            "end_time": None,
            "current_status": "running",
            "error_message": None,
            "total_chunks": 0
        })
        
        print("Starting FineWeb indexing task...")
        
        # Run the indexing process
        chunks = run_fineweb_indexing()
        
        # Update final state
        end_time = datetime.now()
        start_time = datetime.fromisoformat(indexing_state["start_time"])
        duration = (end_time - start_time).total_seconds()
        
        indexing_state.update({
            "is_running": False,
            "end_time": end_time.isoformat(),
            "total_chunks": len(chunks),
            "current_status": "completed",
            "error_message": None
        })
        
        print(f"Indexing completed successfully! {len(chunks)} chunks processed in {duration:.2f} seconds")
        
    except Exception as e:
        error_msg = str(e)
        indexing_state.update({
            "is_running": False,
            "end_time": datetime.now().isoformat(),
            "current_status": "failed",
            "error_message": error_msg
        })
        print(f"Indexing failed: {error_msg}")

@app.post("/start-indexing", response_model=IndexingResult)
async def start_indexing(background_tasks: BackgroundTasks):
    """
    Start the FineWeb indexing process.
    
    Returns:
        IndexingResult: Status of the indexing start request
    """
    global indexing_state
    
    if indexing_state["is_running"]:
        raise HTTPException(
            status_code=409, 
            detail="Indexing is already running. Use /status to check progress."
        )
    
    # Start indexing in background
    thread = threading.Thread(target=run_indexing_task)
    thread.daemon = True
    thread.start()
    
    return IndexingResult(
        success=True,
        message="Indexing started successfully",
        total_chunks=0,
        duration_seconds=0.0
    )

@app.get("/status", response_model=IndexingStatus)
async def get_indexing_status():
    """
    Get the current status of the indexing process.
    
    Returns:
        IndexingStatus: Current indexing status
    """
    global indexing_state
    
    duration_seconds = None
    if indexing_state["start_time"]:
        start_time = datetime.fromisoformat(indexing_state["start_time"])
        end_time_str = indexing_state["end_time"]
        
        if end_time_str:
            end_time = datetime.fromisoformat(end_time_str)
            duration_seconds = (end_time - start_time).total_seconds()
        elif indexing_state["is_running"]:
            duration_seconds = (datetime.now() - start_time).total_seconds()
    
    return IndexingStatus(
        is_running=indexing_state["is_running"],
        status=indexing_state["current_status"],
        start_time=indexing_state["start_time"],
        end_time=indexing_state["end_time"],
        total_chunks=indexing_state["total_chunks"],
        duration_seconds=duration_seconds,
        error_message=indexing_state["error_message"]
    )

@app.post("/stop-indexing")
async def stop_indexing():
    """
    Stop the indexing process (if running).
    
    Note: This is a graceful stop request - actual stopping depends on implementation.
    """
    global indexing_state
    
    if not indexing_state["is_running"]:
        raise HTTPException(
            status_code=409,
            detail="No indexing process is currently running"
        )
    
    # Note: In a production system, you'd implement proper cancellation
    return {
        "message": "Stop request received. Indexing will stop after current batch.",
        "note": "Implementation of graceful stopping is pending"
    }

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "FineWeb Indexer",
        "workers": 4,
        "files_per_worker": 51
    }

@app.get("/")
async def root():
    """Root endpoint with service information."""
    return {
        "service": "FineWeb Indexer Service",
        "description": "Indexes FineWeb training data from Azure Blob Storage",
        "total_files": 205,
        "workers": 4,
        "files_per_worker": 51,
        "chunk_max_words": 300,
        "endpoints": {
            "start_indexing": "POST /start-indexing - Start indexing process",
            "status": "GET /status - Get indexing status",
            "stop_indexing": "POST /stop-indexing - Stop indexing process",
            "health": "GET /health - Health check"
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
