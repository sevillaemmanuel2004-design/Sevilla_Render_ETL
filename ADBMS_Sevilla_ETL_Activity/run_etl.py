import subprocess
import sys

def run_script(script_name):
    print(f"\nRunning {script_name}...")
    result = subprocess.run(
        [sys.executable, script_name],
        check=True
    )
    print(f"{script_name} completed successfully.")

if __name__ == "__main__":
    try:
        run_script("extract.py")
        run_script("transform.py")
        run_script("load.py")
        print("\nETL pipeline finished successfully!")
    except subprocess.CalledProcessError as e:
        print(f"\nETL failed while running: {e.cmd}")
        sys.exit(1)
