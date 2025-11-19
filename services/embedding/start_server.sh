#!/bin/bash

# Embedding Service Startup Script

echo "Starting Embedding Service..."
echo "Model: Qwen/Qwen3-Embedding-4B"
echo "Server will be available at: http://0.0.0.0:8000"
echo "Health check: http://0.0.0.0:8000/health"
echo "API docs: http://0.0.0.0:8000/docs"
echo ""

# Start the FastAPI server
python server.py
