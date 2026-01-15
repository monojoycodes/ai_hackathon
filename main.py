import os
import json
import glob
from core.ingester import Ingester
from core.cleaner import DataCleaner
from core.mapper import SmartMapper

def run_harmonization_pipeline():
    # 1. Initialize Folders
    INPUT_DIR = "uploads"
    OUTPUT_DIR = "outputs"
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    files = glob.glob(f"{INPUT_DIR}/*.csv")
    if not files:
        print(f"Empty '{INPUT_DIR}' folder. Please add CSV files.")
        return

    # 2. Load Modules
    ingester = Ingester()
    cleaner = DataCleaner()
    mapper = SmartMapper()

    print(f"🚀 AIKosh Harmonizer Started. Processing {len(files)} files...")

    for file_path in files:
        fname = os.path.basename(file_path)
        print(f"\n--- Processing: {fname} ---")

        # STEP A: INGEST (Full Data)
        df = ingester.load_file(file_path)
        if df is None: continue

        # STEP B: CLEAN (Deterministic)
        df = cleaner.clean(df)

        # STEP C: AI HARMONIZATION (Mapping Logic)
        # Passing a sample (head) so AI understands the context of the columns
        ai_logic = mapper.get_transformation_logic(
            fname, 
            list(df.columns), 
            df.head(5).to_string()
        )

        if ai_logic:
            mapping = ai_logic.get("column_mapping", {})
            metadata = ai_logic.get("metadata", {})

            # STEP D: TRANSFORM (The actual Data Harmonization)
            # 1. Rename columns
            df_final = df.rename(columns=mapping)
            
            # 2. Remove columns marked for dropping
            cols_to_keep = [c for c in df_final.columns if c != "DROP"]
            df_final = df_final[cols_to_keep]
            
            # STEP E: SAVE OUTPUTS
            base_name = os.path.splitext(fname)[0]
            csv_out = f"{OUTPUT_DIR}/{base_name}_harmonized.csv"
            json_out = f"{OUTPUT_DIR}/{base_name}_metadata.json"

            try:
                # Save CSV (The Harmonized Data)
                df_final.to_csv(csv_out, index=False)
                
                # Save JSON (The Metadata)
                with open(json_out, "w") as f:
                    json.dump(metadata, f, indent=2)

                print(f"✅ SUCCESS! Saved to {csv_out}")

            except PermissionError:
                print(f"❌ ERROR: Could not save {csv_out}. Please close the file if it's open in Excel!")
                continue # Skip to the next file in the 10-file list

if __name__ == "__main__":
    run_harmonization_pipeline()