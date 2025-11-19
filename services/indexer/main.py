import subprocess
import sys
from pathlib import Path

def main():
    print("Hello from indexer!")
    print("Starting FineWeb indexer server...")
    
    # Get the directory of this script
    script_dir = Path(__file__).parent
    
    try:
        # Run the server directly
        result = subprocess.run([
            sys.executable, "server.py"
        ], cwd=script_dir, check=True)
        return result.returncode
    except subprocess.CalledProcessError as e:
        print(f"Error starting server: {e}")
        return e.returncode
    except Exception as e:
        print(f"Unexpected error: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
