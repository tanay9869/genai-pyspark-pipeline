#!/usr/bin/env python3
"""
Wrapper script to run the file format benchmarking.
This script automatically activates the virtual environment and runs the benchmark.
"""

import subprocess
import sys
import os

def main():
    """Run the benchmark script with proper environment setup."""
    try:
        # Get the directory of this script
        script_dir = os.path.dirname(os.path.abspath(__file__))
        venv_dir = os.path.join(script_dir, "venv")

        # Check if virtual environment exists
        if not os.path.exists(venv_dir):
            print("Error: Virtual environment not found. Please run setup first.")
            sys.exit(1)

        # Activate virtual environment and run benchmark
        if os.name == 'nt':  # Windows
            python_exe = os.path.join(venv_dir, "Scripts", "python.exe")
            activate_script = os.path.join(venv_dir, "Scripts", "activate.bat")
        else:  # Unix/Linux/MacOS
            python_exe = os.path.join(venv_dir, "bin", "python")
            activate_script = os.path.join(venv_dir, "bin", "activate")

        # Run the benchmark script directly with the virtual environment's Python
        benchmark_script = os.path.join(script_dir, "benchmark_formats.py")
        result = subprocess.run([python_exe, benchmark_script], cwd=script_dir)

        sys.exit(result.returncode)

    except Exception as e:
        print(f"Error running benchmark: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()