#!/bin/bash

# Test script for convert_chunk_to_embedding function
echo "=== Testing Qwen3-Embedding-8B Model ==="
echo ""

# Check if we're in the right directory
if [ ! -f "convert_embedding.py" ]; then
    echo "Error: convert_embedding.py not found. Make sure you're in the embedding service directory."
    exit 1
fi

# Check if model exists locally
MODEL_PATH="../../../models/Qwen3-Embedding-8B"
if [ -d "$MODEL_PATH" ]; then
    echo "✓ Local model found at: $MODEL_PATH"
else
    echo "⚠ Local model not found. Will download from HuggingFace Hub on first run."
fi

echo ""
echo "Running embedding test..."
echo "========================="

# Run the test
python3 convert_embedding.py

echo ""
echo "Test completed!"