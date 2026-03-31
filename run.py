#!/usr/bin/env python3
"""
Wrapper script to run the main data generation pipeline.
This script can be run with python3 and will handle virtual environment activation.
"""

import sys
import os
import subprocess
from pathlib import Path

def main():
    """Run the main pipeline with proper environment setup."""
    project_root = Path(__file__).parent
    venv_python = project_root / "venv" / "Scripts" / "python.exe"

    if not venv_python.exists():
        print("Error: Virtual environment not found at venv/Scripts/python.exe")
        print("Please run: python -m venv venv")
        print("Then: pip install -r requirements.txt")
        sys.exit(1)

    # Check if we're already in the virtual environment
    if sys.executable == str(venv_python):
        # Already in venv, run main.py directly
        subprocess.run([sys.executable, "main.py"])
    else:
        # Not in venv, run with venv python
        subprocess.run([str(venv_python), "main.py"])

if __name__ == "__main__":
    main()