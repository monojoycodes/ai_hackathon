"""
Simple script to rename harmonized files using titles from metadata
No LLM needed - just reads existing JSON files
"""
import os
import json
import re
from pathlib import Path
import shutil

def sanitize_filename(title):
    """
    Convert title to safe filename
    Example: "District-wise Livestock Census - Maharashtra (2023)"
    → "district_wise_livestock_census_maharashtra_2023"
    """
    # Convert to lowercase
    filename = title.lower()
    
    # Remove special characters, keep only alphanumeric and spaces
    filename = re.sub(r'[^a-z0-9\s]', '', filename)
    
    # Replace multiple spaces with single space
    filename = re.sub(r'\s+', ' ', filename)
    
    # Replace spaces with underscores
    filename = filename.replace(' ', '_')
    
    # Remove leading/trailing underscores
    filename = filename.strip('_')
    
    # Limit length to 100 characters
    if len(filename) > 100:
        filename = filename[:100].rsplit('_', 1)[0]
    
    return filename

def rename_files():
    """
    Rename all harmonized files using titles from metadata
    """
    harmonized_dir = 'outputs/harmonized'
    
    if not os.path.exists(harmonized_dir):
        print(f"❌ Directory not found: {harmonized_dir}")
        return
    
    print("\n" + "="*70)
    print("🔄 Renaming Harmonized Files")
    print("="*70)
    
    # Find all metadata JSON files
    json_files = list(Path(harmonized_dir).glob('*_metadata.json'))
    
    if not json_files:
        print("\n❌ No metadata files found")
        return
    
    print(f"\nFound {len(json_files)} files to process\n")
    
    renamed_count = 0
    skipped_count = 0
    
    for json_path in json_files:
        try:
            # Read metadata JSON
            with open(json_path, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
            
            # Extract title from catalog_info
            title = metadata.get('catalog_info', {}).get('title')
            
            if not title or title == '':
                print(f"⚠️  No title in {json_path.name}, skipping")
                skipped_count += 1
                continue
            
            # Create safe filename from title
            new_base_name = sanitize_filename(title)
            
            if not new_base_name:
                print(f"⚠️  Could not create filename from title: '{title}', skipping")
                skipped_count += 1
                continue
            
            # Find corresponding CSV file
            # Current JSON name: something_metadata.json
            # Corresponding CSV: something.csv or something_harmonized.csv
            original_base = json_path.stem.replace('_metadata', '')
            
            # Look for CSV file
            csv_candidates = [
                Path(harmonized_dir) / f"{original_base}.csv",
                Path(harmonized_dir) / f"{original_base}_harmonized.csv"
            ]
            
            csv_path = None
            for candidate in csv_candidates:
                if candidate.exists():
                    csv_path = candidate
                    break
            
            if not csv_path:
                print(f"⚠️  No CSV found for {json_path.name}, skipping")
                skipped_count += 1
                continue
            
            # Define new paths
            new_csv_path = Path(harmonized_dir) / f"{new_base_name}.csv"
            new_json_path = Path(harmonized_dir) / f"{new_base_name}.json"
            
            # Check if files already exist with new name
            if new_csv_path.exists() or new_json_path.exists():
                # Add number suffix
                counter = 1
                while True:
                    new_csv_path = Path(harmonized_dir) / f"{new_base_name}_{counter}.csv"
                    new_json_path = Path(harmonized_dir) / f"{new_base_name}_{counter}.json"
                    if not new_csv_path.exists() and not new_json_path.exists():
                        break
                    counter += 1
                print(f"⚠️  Filename exists, using: {new_base_name}_{counter}")
                new_base_name = f"{new_base_name}_{counter}"
            
            # Display what we're doing
            print(f"\n📝 Title: {title}")
            print(f"   Old CSV: {csv_path.name}")
            print(f"   New CSV: {new_csv_path.name}")
            print(f"   Old JSON: {json_path.name}")
            print(f"   New JSON: {new_json_path.name}")
            
            # Rename files
            shutil.move(str(csv_path), str(new_csv_path))
            shutil.move(str(json_path), str(new_json_path))
            
            print(f"   ✅ Renamed successfully")
            renamed_count += 1
            
        except json.JSONDecodeError as e:
            print(f"❌ Invalid JSON in {json_path.name}: {e}")
            skipped_count += 1
        except Exception as e:
            print(f"❌ Error processing {json_path.name}: {e}")
            skipped_count += 1
    
    # Summary
    print("\n" + "="*70)
    print(f"✅ Renamed: {renamed_count} files")
    if skipped_count > 0:
        print(f"⚠️  Skipped: {skipped_count} files")
    print("="*70)

if __name__ == "__main__":
    rename_files()