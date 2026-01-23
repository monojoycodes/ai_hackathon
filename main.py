import os
import json
import glob
import re
import shutil
from pathlib import Path
from core.ingester import Ingester
from core.cleaner import DataCleaner
from core.schema_generator import UnifiedSchemaGenerator
from core.transformer import SchemaTransformer
from core.metadata_generator import MetadataGenerator

def sanitize_filename(title):
    """Convert title to safe filename"""
    filename = title.lower()
    filename = re.sub(r'[^a-z0-9\s]', '', filename)
    filename = re.sub(r'\s+', '_', filename)
    filename = filename.strip('_')
    if len(filename) > 100:
        filename = filename[:100].rsplit('_', 1)[0]
    return filename

def rename_harmonized_files(harmonized_dir):
    """
    Rename harmonized files using titles from metadata
    """
    print("\n" + "="*70)
    print("🔄 Renaming Files Using AI-Generated Titles...")
    print("="*70)
    
    # Find all metadata JSON files
    json_files = list(Path(harmonized_dir).glob('*_metadata.json'))
    
    if not json_files:
        print("⚠️  No metadata files found to rename")
        return
    
    print(f"\nProcessing {len(json_files)} files...\n")
    
    renamed_count = 0
    
    for json_path in json_files:
        try:
            # Read metadata
            with open(json_path, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
            
            # Extract title
            title = metadata.get('catalog_info', {}).get('title')
            
            if not title:
                print(f"⚠️  No title in {json_path.name}, skipping")
                continue
            
            # Create safe filename
            new_base_name = sanitize_filename(title)
            
            if not new_base_name:
                print(f"⚠️  Invalid title: '{title}', skipping")
                continue
            
            # Find corresponding CSV
            original_base = json_path.stem.replace('_metadata', '')
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
                print(f"⚠️  No CSV found for {json_path.name}")
                continue
            
            # New paths
            new_csv_path = Path(harmonized_dir) / f"{new_base_name}.csv"
            new_json_path = Path(harmonized_dir) / f"{new_base_name}.json"
            
            # Handle duplicates
            if new_csv_path.exists() or new_json_path.exists():
                counter = 1
                while True:
                    new_csv_path = Path(harmonized_dir) / f"{new_base_name}_{counter}.csv"
                    new_json_path = Path(harmonized_dir) / f"{new_base_name}_{counter}.json"
                    if not new_csv_path.exists() and not new_json_path.exists():
                        break
                    counter += 1
            
            # Display
            print(f"📝 {title[:60]}...")
            print(f"   → {new_csv_path.name}")
            
            # Rename
            shutil.move(str(csv_path), str(new_csv_path))
            shutil.move(str(json_path), str(new_json_path))
            
            renamed_count += 1
            
        except Exception as e:
            print(f"❌ Error: {json_path.name}: {e}")
    
    print(f"\n✅ Renamed {renamed_count}/{len(json_files)} files")

def run_two_phase_harmonization():
    """
    Two-Phase Harmonization Pipeline:
    Phase 1: Analyze ALL files → Generate unified schema
    Phase 2: Apply unified schema to each file
    Phase 3: Rename files using AI-generated titles
    """
    
    # Setup
    INPUT_DIR = "uploads"
    OUTPUT_DIR = "outputs/harmonized"
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs("outputs", exist_ok=True)  # Ensure parent dir exists
    
    print("\n" + "="*70)
    print("🚀 AIKosh Metadata Sentinel - Two-Phase Harmonization")
    print("="*70)
    
    # Initialize modules
    ingester = Ingester()
    cleaner = DataCleaner()
    
    # NEW: Initialize Phase 2 modules
    from core.portal_scraper import PortalExtractor
    from core.stats_extractor import StatsExtractor
    
    portal_extractor = PortalExtractor(output_dir=INPUT_DIR)
    stats_extractor = StatsExtractor()
    
    # Store scraped metadata
    dataset_metadata = None
    
    # User Choice: Scrape or Local
    print("\n🔍 SOURCE SELECTION:")
    print("   [1] Scrape from data.gov.in (Enter URL)")
    print("   [2] Process existing files in 'uploads/' folder")
    choice = input("\n   Enter your choice (1 or 2): ").strip()
    
    if choice == '1':
        url = input("   🔗 Enter dataset URL: ").strip()
        if url:
            print(f"   PLEASE WAIT: Scraping metadata and downloading files...")
            extraction_result = portal_extractor.extract_from_url(url)
            
            if extraction_result.get('success'):
                resources = extraction_result.get('resources', [])
                dataset_metadata = extraction_result.get('metadata', {})
                
                if not resources:
                    print(f"\n   ⚠️  AUTH WALL DETECTED: Files could not be auto-downloaded.")
                    print(f"   ✅ However, METADATA was successfully scraped: '{dataset_metadata.get('title')}'")
                    print(f"   👉 ACTION REQUIRED: Please download the CSV manually from the site")
                    print(f"   👉 Save it to 'uploads/' folder, then press Enter.")
                    input("   Press Enter when file is saved...")
                else:
                    print(f"   ✅ Extraction successful! Files saved to 'uploads/'")
            else:
                print(f"   ❌ Extraction failed: {extraction_result.get('error')}")
                return
    else:
        print("   📂 Using local files...")
    
    # =========================================================================
    # PHASE 1: ANALYZE ALL FILES → GENERATE UNIFIED SCHEMA
    # =========================================================================
    
    print("\n" + "="*70)
    print("📊 PHASE 1: Collecting data from all files...")
    print("="*70)
    
    all_file_info = []
    
    files = glob.glob(f"{INPUT_DIR}/*.csv")
    
    if not files:
        print(f"❌ No CSV files found in '{INPUT_DIR}' folder")
        print("Please add CSV files and run again")
        return
        
    print(f"Found {len(files)} files to process\n")
    
    for file_path in files:
        filename = os.path.basename(file_path)
        print(f"\n📄 Reading: {filename}")
        
        # Load file
        df = ingester.load_file(file_path)
        if df is None:
            print(f"   ❌ Skipped")
            continue
        
        # Clean
        df = cleaner.clean(df)
        
        # Extract info
        file_info = ingester.get_file_info(file_path, df)
        file_info['cleaned_df'] = df
        
        # Extract deterministic stats
        file_info['stats'] = stats_extractor.extract_stats(df)
        print(f"   📊 Stats: {file_info['stats']['temporal']['range_str']} | {file_info['stats']['spatial']['granularity']}")
        
        all_file_info.append(file_info)
        
        print(f"   ✓ Columns: {df.columns.tolist()}")
    
    if not all_file_info:
        print("\n❌ No files could be processed")
        return
    
    # Generate unified schema
    schema_generator = UnifiedSchemaGenerator()
    unified_schema = schema_generator.generate_unified_schema(all_file_info)
    
    if not unified_schema:
        print("\n❌ Failed to generate unified schema")
        return
    
    # Save unified schema
    schema_path = "outputs/unified_schema.json"
    with open(schema_path, 'w') as f:
        json.dump(unified_schema, f, indent=2)
    print(f"\n📋 Unified schema saved: {schema_path}")
    
    # =========================================================================
    # PHASE 2: APPLY UNIFIED SCHEMA TO EACH FILE
    # =========================================================================
    
    print("\n" + "="*70)
    print("⚙️  PHASE 2: Applying unified schema to all files...")
    print("="*70)
    
    transformer = SchemaTransformer(unified_schema)
    metadata_generator = MetadataGenerator(unified_schema)
    
    results = []
    metadata_catalog = []
    
    for file_info in all_file_info:
        filename = file_info['filename']
        df = file_info['cleaned_df']
        
        # Transform
        df_harmonized, mapping = transformer.transform(df, filename)
        
        # Save harmonized CSV
        base_name = os.path.splitext(filename)[0]
        output_csv = f"{OUTPUT_DIR}/{base_name}_harmonized.csv"
        
        try:
            df_harmonized.to_csv(output_csv, index=False)
            print(f"   ✅ Saved: {output_csv}")
        except PermissionError:
            print(f"   ❌ ERROR: Could not save {output_csv} (file may be open in Excel)")
            continue
        
        # Generate metadata
        metadata = metadata_generator.generate_metadata(
            file_info, 
            df_harmonized,
            {'mapping': mapping},
            scraped_metadata=dataset_metadata, 
            stats=file_info['stats']
        )
        
        # Save individual metadata
        output_meta = f"{OUTPUT_DIR}/{base_name}_metadata.json"
        with open(output_meta, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        metadata_catalog.append(metadata)
        
        results.append({
            'original': filename,
            'harmonized': output_csv,
            'columns': df_harmonized.columns.tolist()
        })
    
    # =========================================================================
    # PHASE 3: VERIFICATION & OUTPUTS
    # =========================================================================
    
    print("\n" + "="*70)
    print("✅ VERIFICATION: Checking schema consistency...")
    print("="*70)
    
    if results:
        first_cols = results[0]['columns']
        all_consistent = True
        
        for result in results[1:]:
            if result['columns'] != first_cols:
                print(f"❌ INCONSISTENCY: {result['original']}")
                all_consistent = False
        
        if all_consistent:
            print(f"\n🎉 SUCCESS! All {len(results)} files have IDENTICAL schemas!")
            print(f"   Standard columns: {first_cols}")
            
            # Try to merge
            try:
                import pandas as pd
                dfs = [pd.read_csv(r['harmonized']) for r in results]
                merged = pd.concat(dfs, ignore_index=True)
                
                merged_path = "outputs/merged_all_data.csv"
                merged.to_csv(merged_path, index=False)
                
                print(f"\n✅ BONUS: Successfully merged all files!")
                print(f"   Merged dataset: {merged_path}")
                print(f"   Total rows: {len(merged):,}")
            except Exception as e:
                print(f"\n⚠️  Merge test failed: {e}")
    
    # Save metadata catalog
    catalog_path = "outputs/metadata_catalog.json"
    with open(catalog_path, 'w') as f:
        json.dump(metadata_catalog, f, indent=2)
    
    # =========================================================================
    # PHASE 4: RENAME FILES USING AI-GENERATED TITLES
    # =========================================================================
    
    rename_harmonized_files(OUTPUT_DIR)
    
    # Final summary
    print("\n" + "="*70)
    print("🏁 HARMONIZATION COMPLETE")
    print("="*70)
    print(f"✅ Processed: {len(results)}/{len(files)} files")
    print(f"📁 Harmonized CSVs: {OUTPUT_DIR}/")
    print(f"📋 Unified schema: outputs/unified_schema.json")
    print(f"📊 Metadata catalog: {catalog_path}")
    print("="*70)

if __name__ == "__main__":
    run_two_phase_harmonization()