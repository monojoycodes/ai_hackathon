import os
import json
from google import genai
from dotenv import load_dotenv

load_dotenv()

class GlobalMapper:
    def __init__(self):
        self.client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

    def create_master_schema(self, all_file_headers):
        """
        PASS 2: The 'Council' Logic. 
        Analyzes headers from ALL files to create ONE unified standard.
        """
        # Context: We pass the structure of all 10 files at once
        context_str = json.dumps(all_file_headers, indent=2)
        
        prompt = f"""
        Act as the Lead Data Architect for the IndiaAI Mission. 
        I have {len(all_file_headers)} distinct CSV files that all belong to the SAME domain (e.g., Water Resources or Livestock Census).
        
        CONTEXT:
        These files are from different districts/operators and use inconsistent naming conventions (e.g., 'dist', 'District_Name', 'Zilla').
        Your job is to unify them into a Single Golden Schema.

        INPUT HEADERS (File by File):
        {context_str}

        STRICT MAPPING RULES:
        1. AGGRESSIVE UNIFICATION: Identify synonyms. 'dist', 'D_Name', 'District', 'Zilla' MUST all map to -> 'district_name'.
        2. SNAKE_CASE ONLY: All target columns must be lowercase_with_underscores.
        3. NO AMBIGUITY: 'Pop', 'Ppl', 'Total' should map to something descriptive like 'population_count'.
        4. DROP JUNK: Map system columns like 'Unnamed: 0', 'id', 'serial_no' to "DROP".
        5. PRESERVE DATES: Ensure any column with 'date' or 'time' in the name is mapped to 'report_date' or similar.

        OUTPUT JSON FORMAT:
        Return a single flat dictionary mapping EVERY unique raw header found in the input to its new standard name.
        Example: 
        {{ 
            "dist": "district_name", 
            "District": "district_name", 
            "Zilla": "district_name", 
            "lat": "latitude",
            "sr_no": "DROP" 
        }}
        """
        
        try:
            print("   🧠 Gemma is building the Golden Schema across all files...")
            response = self.client.models.generate_content(
                model="gemma-3-27b-it", 
                contents=prompt
            )
            
            clean_text = response.text.strip()
            if "```json" in clean_text:
                clean_text = clean_text.split("```json")[1].split("```")[0]
            elif "```" in clean_text:
                clean_text = clean_text.split("```")[1].split("```")[0]
                
            return json.loads(clean_text)
            
        except Exception as e:
            print(f"❌ Master Schema Generation Failed: {e}")
            return {}

    def generate_metadata(self, filename, columns, sample_data=""):
        """
        PASS 3: The 'Librarian' Logic.
        Generates deep, specific AIKosh metadata.
        """
        prompt = f"""
        Act as a Senior Data Steward for the India Data Management Office (IDMO).
        Generate the official AIKosh Metadata Catalog entry for this dataset.

        FILE CONTEXT:
        - Filename: {filename}
        - Final Standardized Columns: {columns}
        - Data Sample Hint: {sample_data}

        MANDATORY REQUIREMENTS:
        1. TITLE: Must be formal and specific. (Bad: "Water Data". Good: "District-wise Groundwater Level Monitoring Report - Maharashtra")
        2. MINISTRY INFERENCE: You MUST infer the correct Government of India Ministry (e.g., Ministry of Jal Shakti, MoHFW, Ministry of Agriculture).
        3. SECTOR TAXONOMY: Map to one of: [Agriculture, Healthcare, Water Resources, Rural Development, Education, Energy].
        4. GRANULARITY: Analyze columns to determine LGD level (State > District > Sub-District > Village).
        5. AI READINESS: specific float score (0.0-1.0) based on how clean and standard the columns are.

        OUTPUT STRICT JSON STRUCTURE:
        {{
            "catalog_info": {{
                "title": "Formal Title String",
                "description": "2-sentence technical description focusing on parameters measured and potential AI use-cases (e.g., 'predictive modeling of water scarcity').",
                "sector": "Sector Name",
                "keywords": ["specific_tag1", "specific_tag2", "tag3", "tag4"]
            }},
            "provenance": {{
                "ministry": "Inferred Ministry Name",
                "department": "Inferred Department (e.g., Central Ground Water Board)",
                "jurisdiction": "Inferred State/Region from filename",
                "granularity": "National/State/District/Block/Village"
            }},
            "spatial_temporal": {{
                "spatial_coverage": "Inferred Region",
                "temporal_resolution": "Daily/Monthly/Yearly (Infer from columns like 'year', 'date')"
            }},
            "technical_metadata": {{
                "file_format": "CSV",
                "column_count": {len(columns)},
                "standard_compliance": "NDSAP-2.0",
                "ai_readiness_score": 0.95
            }}
        }}
        """
        try:
            # We use a fresh API call per file to ensure custom metadata for each
            response = self.client.models.generate_content(
                model="gemini-2.0-flash", 
                contents=prompt
            )
            clean_text = response.text.replace("```json", "").replace("```", "").strip()
            return json.loads(clean_text)
        except:
            return {
                "error": "Metadata generation failed", 
                "catalog_info": {"title": filename}
            }