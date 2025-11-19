import subprocess
import sys
import os
from pathlib import Path

def main():
    print("Hello from embedding!")
    print("Starting embedding server...")
    
    # Get the directory of this script
    script_dir = Path(__file__).parent
    start_script = script_dir / "start_server.sh"
    
    try:
        # Run the start server script
        result = subprocess.run([str(start_script)], 
                              cwd=script_dir,
                              check=True)
        return result.returncode
    except subprocess.CalledProcessError as e:
        print(f"Error starting server: {e}")
        return e.returncode
    except FileNotFoundError:
        print(f"Start script not found: {start_script}")
        return 1
    except Exception as e:
        print(f"Unexpected error: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
