#!/usr/bin/env python3
"""
Test script for the FineWeb indexer service.
"""

import requests
import time
import json

def test_indexer_service(base_url="http://localhost:8001"):
    """Test the indexer service endpoints."""
    
    print("=== FineWeb Indexer Service Test ===\n")
    
    # Test health check
    print("1. Testing health check...")
    try:
        response = requests.get(f"{base_url}/health")
        print(f"Health status: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
    except Exception as e:
        print(f"Health check failed: {e}")
        return
    
    # Test service info
    print("\n2. Testing service info...")
    try:
        response = requests.get(f"{base_url}/")
        print(f"Service info: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
    except Exception as e:
        print(f"Service info failed: {e}")
        return
    
    # Test status before starting
    print("\n3. Testing status (before starting)...")
    try:
        response = requests.get(f"{base_url}/status")
        print(f"Status: {response.status_code}")
        print(f"Response: {json.dumps(response.json(), indent=2)}")
    except Exception as e:
        print(f"Status check failed: {e}")
        return
    
    # Start indexing (commented out for safety - uncomment to test actual indexing)
    print("\n4. Starting indexing process...")
    print("WARNING: This will start actual indexing of FineWeb data!")
    print("Uncomment the following lines to test actual indexing:")
    print("# try:")
    print("#     response = requests.post(f'{base_url}/start-indexing')")
    print("#     print(f'Start indexing: {response.status_code}')")
    print("#     print(f'Response: {json.dumps(response.json(), indent=2)}')")
    print("# except Exception as e:")
    print("#     print(f'Start indexing failed: {e}')")
    
    print("\n=== Test completed ===")

if __name__ == "__main__":
    test_indexer_service()
