import pandas as pd
import os
from datetime import datetime
from pydantic import BaseModel, Field
from typing import List
import glob

# 1. Define the internal schema for raw extraction
class RawFileMetadata(BaseModel):
    filename: str
    file_extension: str
    file_size_kb: float
    columns: List[str]
    sample_data: str # We'll send this to Gemma
    last_modified: str

def extract_file_info(file_path):
    # Get OS-level metadata
    stats = os.stat(file_path)
    ext = os.path.splitext(file_path)[1].lower()
    
    # Read content based on extension
    if ext == '.csv':
        df = pd.read_csv(file_path, nrows=5)
    elif ext in ['.xlsx', '.xls']:
        df = pd.read_excel(file_path, nrows=5)
    else:
        raise ValueError("Unsupported format. Please use CSV or XLSX.")

    return RawFileMetadata(
        filename=os.path.basename(file_path),
        file_extension=ext,
        file_size_kb=round(stats.st_size / 1024, 2),
        columns=df.columns.tolist(),
        sample_data=df.head(3).to_string(),
        last_modified=datetime.fromtimestamp(stats.st_mtime).strftime('%Y-%m-%d')
    )

# Test run
if __name__ == "__main__":
    # Ensure the folder exists so the script doesn't just sit there
    if not os.path.exists("uploads"):
        os.makedirs("uploads")
        print("Created 'uploads' folder. Put your CSV files there and run again.")

    files = glob.glob("uploads/*.csv")
    
    if not files:
        print("No CSV files found in 'uploads' folder.")
    
    for f in files:
        print(f"\n[SCANNING] Processing file: {f}")
        
        # This is where the extraction happens
        raw_meta = extract_file_info(f)
        
        # --- ADD THESE LINES TO SEE OUTPUT ---
        print("-" * 30)
        print(f"Metadata for {raw_meta.filename}:")
        # .model_dump_json(indent=2) makes the Pydantic output readable
        print(raw_meta.model_dump_json(indent=2)) 
        print("-" * 30)