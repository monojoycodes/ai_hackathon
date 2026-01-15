import os
import json
from google import genai
from dotenv import load_dotenv

load_dotenv()

class SmartMapper:
    def __init__(self):
        # Using the standard SDK client
        self.client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

    def get_transformation_logic(self, filename, columns, sample_rows):
        """Asks Gemma to define the mapping and metadata for the file."""
        prompt = f"""
Act as a Senior Data Architect for the IndiaAI Mission (AIKosh).
Your goal is to transform raw file signals into a high-fidelity, production-ready Metadata Schema.

RAW INPUT:
- Filename: {filename}
- Headers: {columns}
- Data Preview: {sample_rows}

TASK:
1. HARMONIZATION: Map raw headers to standard_snake_case.
2. CATEGORIZATION: Map the dataset to the official AIKosh Sector taxonomy.
3. PROVENANCE: Identify the specific Indian Ministry and State/District jurisdiction.
4. GRANULARITY: Is this State-level, District-level, or Village-level data?

STRICT JSON OUTPUT STRUCTURE:
{{
    "column_mapping": {{ "old_col": "new_col" }},
    "metadata": {{
        "catalog_info": {{
            "title": "Formal name (e.g., 'National Water Quality Monitoring - Maharashtra State')",
            "description": "2-sentence professional summary including the specific utility for AI modeling.",
            "sector": "Choose ONE: [Agriculture, Livestock, Health, Water & Sanitation, Education, Rural Development]",
            "keywords": ["tag1", "tag2", "tag3"]
        }},
        "provenance": {{
            "ministry": "Inferred official Ministry (e.g., Ministry of Jal Shakti)",
            "department": "Inferred Department",
            "jurisdiction": "Specific State/District mentioned",
            "granularity": "Choose: [National, State, District, Block, Village]"
        }},
        "spatial_temporal": {{
            "temporal_coverage": "YYYY-MM-DD to YYYY-MM-DD (Infer if possible)",
            "spatial_coverage": "Geographic extent"
        }},
        "technical_metadata": {{
            "ai_readiness_level": "Float 0.0-1.0 based on structuredness",
            "is_machine_readable": true,
            "standard_conformity": "NDSAP/IDMO"
        }}
    }}
}}
"""
        
        try:
            # We use gemini-2.0-flash for speed and JSON reliability
            response = self.client.models.generate_content(
                model="gemma-3-27b-it", 
                contents=prompt
            )
            
            # Extract JSON from potential markdown backticks
            clean_text = response.text.strip()
            if "```json" in clean_text:
                clean_text = clean_text.split("```json")[1].split("```")[0]
            elif "```" in clean_text:
                clean_text = clean_text.split("```")[1].split("```")[0]
                
            return json.loads(clean_text)
        except Exception as e:
            print(f"⚠️ AI Mapping Error: {e}")
            return None