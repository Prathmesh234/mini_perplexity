import subprocess
import sys
from pathlib import Path

def main():
    print("Starting Retriever Service...")
    
    # Get the directory of this script
    script_dir = Path(__file__).parent
    
    # Command to run uvicorn
    # We run it as a module to ensure it uses the same python environment
    server_cmd = [
        sys.executable, "-m", "uvicorn", 
        "server:app", 
        "--host", "0.0.0.0", 
        "--port", "8002",
        # "--reload" # Reload causes issues with background processes sometimes, better to run directly in prod-like setup
    ]
    
    cache_manager_process = None
    
    try:
        # Start Cache Manager in background
        print("Starting Cache Manager...")
        cache_manager_cmd = [sys.executable, "cache_manager.py"]
        cache_manager_process = subprocess.Popen(cache_manager_cmd, cwd=script_dir)
        
        # Run uvicorn (blocking)
        result = subprocess.run(server_cmd, cwd=script_dir, check=False)
        return result.returncode
        
    except KeyboardInterrupt:
        print("\nStopping Retriever Service...")
        return 0
    except Exception as e:
        print(f"Error starting server: {e}")
        return 1
    finally:
        # Ensure cache manager is killed
        if cache_manager_process:
            print("Stopping Cache Manager...")
            cache_manager_process.terminate()
            try:
                cache_manager_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                cache_manager_process.kill()

if __name__ == "__main__":
    sys.exit(main())
