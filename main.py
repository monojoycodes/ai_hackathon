import os
import json
import glob
from core.ingester import Ingester
from core.cleaner import DataCleaner
from core.schema_generator import UnifiedSchemaGenerator
from core.transformer import SchemaTransformer
from core.metadata_generator import MetadataGenerator

def run_two_phase_harmonization():
    """
    Two-Phase Harmonization Pipeline:
    Phase 1: Analyze ALL files → Generate unified schema
    Phase 2: Apply unified schema to each file
    """
    
    # Setup
    INPUT_DIR = "uploads"
    OUTPUT_DIR = "outputs/harmonized"
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    
    # files = glob.glob(f"{INPUT_DIR}/*.csv")
    
    # if not files:
    #     print(f"❌ No CSV files found in '{INPUT_DIR}' folder")
    #     print("Please add CSV files and run again")
    #     return
    
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
    
    # Store scraped metadata to pass to the generator later
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
                
                # Check if we got metadata but NO files (Auth Wall scenario)
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
        file_info['cleaned_df'] = df  # Store for Phase 2
        
        # NEW: Extract deterministic stats
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
        
        # Generate metadata (Hybrid Approach)
        # Note: In a real run, 'scraped_metadata' would come from portal_extractor results
        # We pass the scraped metadata (if any) to help formatting
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
                print(f"   Expected: {first_cols}")
                print(f"   Got: {result['columns']}")
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
        else:
            print(f"\n❌ Schema inconsistency detected!")
    
    # Save metadata catalog
    catalog_path = "outputs/metadata_catalog.json"
    with open(catalog_path, 'w') as f:
        json.dump(metadata_catalog, f, indent=2)
    
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



#     # =========================================================================
# # PHASE 3: VERIFICATION & OUTPUTS
# # =========================================================================

# print("\n" + "="*70)
# print("✅ VERIFICATION: Checking schema consistency...")
# print("="*70)

# if results:
#     # Get core columns (columns that appear in most files)
#     from collections import Counter
    
#     # Count column frequency
#     all_columns = []
#     for result in results:
#         all_columns.extend(result['columns'])
    
#     column_freq = Counter(all_columns)
    
#     # Core columns are those that appear in 70%+ of files
#     threshold = len(results) * 0.7
#     core_columns = [col for col, count in column_freq.items() if count >= threshold]
#     core_columns.sort()  # Sort for consistent ordering
    
#     print(f"\n📊 Core columns (appear in {int(threshold)}/{len(results)} files):")
#     print(f"   {core_columns}")
    
#     # Check if all files have the core columns
#     all_have_core = True
#     files_missing_core = []
    
#     for result in results:
#         missing = [col for col in core_columns if col not in result['columns']]
#         if missing:
#             all_have_core = False
#             files_missing_core.append({
#                 'file': result['original'],
#                 'missing': missing
#             })
    
#     if all_have_core:
#         print(f"\n🎉 SUCCESS! All {len(results)} files have the CORE schema!")
#         print(f"   Core columns: {core_columns}")
        
#         # Show files with extra columns
#         files_with_extra = []
#         for result in results:
#             extra = [col for col in result['columns'] if col not in core_columns]
#             if extra:
#                 files_with_extra.append({
#                     'file': result['original'],
#                     'extra': extra
#                 })
        
#         if files_with_extra:
#             print(f"\n📋 Files with additional columns (this is OK):")
#             for item in files_with_extra:
#                 print(f"   {item['file']}: +{item['extra']}")
        
#         # Try to merge
#         try:
#             import pandas as pd
#             dfs = [pd.read_csv(r['harmonized']) for r in results]
#             merged = pd.concat(dfs, ignore_index=True)
            
#             merged_path = "outputs/merged_all_data.csv"
#             merged.to_csv(merged_path, index=False)
            
#             print(f"\n✅ BONUS: Successfully merged all files!")
#             print(f"   Merged dataset: {merged_path}")
#             print(f"   Total rows: {len(merged):,}")
#             print(f"   Total columns: {len(merged.columns)}")
            
#             # Data quality report
#             print(f"\n📊 Data Quality Report:")
#             print(f"   Total cells: {len(merged) * len(merged.columns):,}")
#             print(f"   Non-null cells: {merged.count().sum():,}")
#             print(f"   Null cells: {merged.isnull().sum().sum():,}")
#             completeness = (merged.count().sum() / (len(merged) * len(merged.columns))) * 100
#             print(f"   Completeness: {completeness:.1f}%")
            
#         except Exception as e:
#             print(f"\n⚠️  Merge test failed: {e}")
#     else:
#         print(f"\n⚠️  Some files missing core columns:")
#         for item in files_missing_core:
#             print(f"   {item['file']}: missing {item['missing']}")