import subprocess
import sys
from pathlib import Path

def main():
    print("Starting Retriever Service...")
    
    # Get the directory of this script
    script_dir = Path(__file__).parent
    
    # Command to run uvicorn
    # We run it as a module to ensure it uses the same python environment
    cmd = [
        sys.executable, "-m", "uvicorn", 
        "server:app", 
        "--host", "0.0.0.0", 
        "--port", "8002",
        "--reload"
    ]
    
    try:
        # Run uvicorn
        result = subprocess.run(cmd, cwd=script_dir, check=False)
        return result.returncode
    except KeyboardInterrupt:
        print("\nStopping Retriever Service...")
        return 0
    except Exception as e:
        print(f"Error starting server: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
