import os
from pathlib import Path

# Define root relative to this file
PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
