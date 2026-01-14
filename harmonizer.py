import os
import json
import glob
from dotenv import load_dotenv
from google import genai
from ingester import extract_file_info  # Import your working ingester logic

# --- 1. SETUP & CONFIG ---
load_dotenv()
api_key = os.getenv("GEMINI_API_KEY")
client = genai.Client(api_key=api_key)

# The specific Gemma model ID for API access
MODEL_ID = "gemma-3-27b-it" 

# --- 2. THE HARMONIZATION LOGIC ---
def get_aikosh_metadata(raw_data):
    """
    Sends raw metadata to Gemma and forces it to return an AIKosh-compatible JSON.
    """
    prompt = f"""
Act as a Senior Data Architect for AIKosh (IndiaAI Mission).
Standardize the following raw metadata into a production-ready JSON object compatible with the India Data Management Office (IDMO) framework.

RAW INPUT:
- Filename: {raw_data.filename}
- Headers: {raw_data.columns}
- Data Preview: {raw_data.sample_data}

OUTPUT JSON STRUCTURE REQUIRED:
{{
    "catalog_info": {{
        "title": "Formal name (e.g., 'Livestock Census - Hingoli District')",
        "description": "Professional 2-sentence summary including the purpose of data.",
        "sector": "Choose from [Agriculture, Livestock, Health, Education, Finance, Energy]",
        "keywords": ["tag1", "tag2", "tag3"]
    }},
    "provenance": {{
        "source": "Inferred Ministry/Department (e.g., Ministry of Fisheries, Animal Husbandry & Dairying)",
        "jurisdiction": "State/District name mentioned in data",
        "data_owner": "Local/State Government of [State]"
    }},
    "spatial_temporal": {{
        "temporal_range": "e.g., '2023-01-01 to 2023-12-31'",
        "spatial_coverage": "Inferred (District/State)",
        "granularity": "Choose from [Village, Block, District, State, National]"
    }},
    "technical_metadata": {{
        "format": "CSV",
        "schema_details": [{{ "column": "name", "type": "inferred type" }}],
        "ai_readiness_level": "Float 0.0-1.0",
        "machine_readable": true
    }}
}}

INSTRUCTIONS:
1. Infer the 'Ministry' based on the 'Sector'.
2. If column names are cryptic (e.g., 'Hing_01'), expand them to human-readable names in 'schema_details'.
3. Output ONLY the raw JSON. No markdown backticks, no preamble.
"""
    try:
        response = client.models.generate_content(
            model=MODEL_ID,
            contents=prompt
        )
        
        # --- 3. RESPONSE CLEANING (Critical for Gemma) ---
        raw_text = response.text.strip()
        
        # Remove markdown backticks if Gemma adds them
        if "```json" in raw_text:
            raw_text = raw_text.split("```json")[1].split("```")[0]
        elif "```" in raw_text:
            raw_text = raw_text.split("```")[1].split("```")[0]
            
        return json.loads(raw_text.strip())
        
    except Exception as e:
        return {"error": f"Harmonization failed: {str(e)}", "raw_output": response.text if 'response' in locals() else None}

# --- 4. EXECUTION BLOCK ---
if __name__ == "__main__":
    # Look for files in your 'uploads/' directory as shown in your folder schema
    files = glob.glob("uploads/*.csv")
    
    if not files:
        print("No files found in 'uploads/'. Please check your folder path.")
    
    for file_path in files:
        print(f"\n[STEP 1] Ingesting: {os.path.basename(file_path)}...")
        raw_info = extract_file_info(file_path) # Uses your working ingester
        
        print("[STEP 2] Harmonizing with Gemma API...")
        final_metadata = get_aikosh_metadata(raw_info)
        
        print("\n--- FINAL AIKOSH COMPATIBLE METADATA ---")
        print(json.dumps(final_metadata, indent=2))
        
        # Optional: Save to a JSON file for your hackathon submission
        output_name = f"metadata_{raw_info.filename.replace('.csv', '.json')}"
        with open(output_name, "w") as f:
            json.dump(final_metadata, f, indent=2)
        print(f"Saved to: {output_name}")