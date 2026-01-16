import os
import json
from google import genai
from dotenv import load_dotenv

load_dotenv()

class MetadataGenerator:
    """
    Generates AIKosh/IDMO-compliant metadata for harmonized datasets.
    """
    
    def __init__(self, unified_schema):
        self.client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
        self.schema = unified_schema
    
    def generate_metadata(self, file_info, harmonized_df, transformation_log):
        """
        Generate rich metadata for a harmonized file.
        
        Args:
            file_info: Original file information
            harmonized_df: The harmonized DataFrame
            transformation_log: Dict with mapping info
        
        Returns:
            AIKosh-compliant metadata dict
        """
        
        prompt = self._build_metadata_prompt(file_info, harmonized_df, transformation_log)
        
        try:
            response = self.client.models.generate_content(
                model="gemma-3-27b-it",
                contents=prompt
            )
            
            metadata = self._parse_response(response.text)
            
            # Add technical metadata
            metadata['technical_metadata'] = {
                'format': 'CSV',
                'original_filename': file_info['filename'],
                'row_count': len(harmonized_df),
                'column_count': len(harmonized_df.columns),
                'columns': harmonized_df.columns.tolist(),
                'data_quality': {
                    'completeness': round(1 - (harmonized_df.isnull().sum().sum() / 
                                              (len(harmonized_df) * len(harmonized_df.columns))), 2),
                    'standardized': True,
                    'ai_ready': True
                },
                'transformations_applied': transformation_log.get('mapping', {})
            }
            
            return metadata
            
        except Exception as e:
            print(f"   ⚠️  Metadata generation warning: {e}")
            return self._fallback_metadata(file_info, harmonized_df)
    
    def _build_metadata_prompt(self, file_info, df, transformation_log):
        """Build AIKosh-compliant metadata prompt"""
        
        return f"""You are a metadata expert for AIKosh (IndiaAI Mission).

DATASET INFORMATION:
- Original filename: {file_info['filename']}
- Harmonized columns: {df.columns.tolist()}
- Sample data:
{df.head(2).to_string()}
- Domain: {self.schema.get('domain', 'Unknown')}
- Sector: {self.schema.get('data_standards', {}).get('sector', 'Unknown')}

YOUR TASK:
Generate AIKosh/IDMO-compliant metadata for this harmonized dataset.

OUTPUT JSON STRUCTURE (STRICT - NDSAP/IDMO Standards):
{{
    "catalog_info": {{
        "title": "Professional, formal title (e.g., 'District-wise Livestock Census - Maharashtra (2023)')",
        "description": "Clear 2-3 sentence description explaining what data this contains and its purpose for AI/ML applications.",
        "sector": "{self.schema.get('data_standards', {}).get('sector', 'Agriculture')}",
        "keywords": ["keyword1", "keyword2", "keyword3", "keyword4"],
        "language": "English",
        "update_frequency": "Annual/Quarterly/Monthly/One-time"
    }},
    
    "provenance": {{
        "source": "{self.schema.get('data_standards', {}).get('ministry', 'Government of India')}",
        "publisher": "Inferred department/authority",
        "jurisdiction": "Inferred State/District from data",
        "data_owner": "Government of [State/India]",
        "contact": "Not available"
    }},
    
    "spatial_temporal": {{
        "temporal_coverage": "Infer year/date range from data (e.g., '2023' or '2020-2023')",
        "spatial_coverage": "Infer geographic scope (e.g., 'Maharashtra State' or 'Multiple States')",
        "granularity": "{self.schema.get('data_standards', {}).get('jurisdiction_level', 'District')}"
    }},
    
    "usage": {{
        "license": "Open Government Data License - India",
        "access_constraints": "None - Public Data",
        "use_cases": ["AI/ML training", "Policy analysis", "Research"],
        "target_audience": ["Researchers", "Data Scientists", "Policy Makers", "Startups"]
    }}
}}

RULES:
- Infer missing information from the data itself
- Be specific about geographic scope if visible in data
- Identify year/temporal range from data
- Return ONLY valid JSON, no markdown
"""
    
    def _parse_response(self, response_text):
        """Parse LLM response"""
        try:
            cleaned = response_text.strip()
            if "```json" in cleaned:
                cleaned = cleaned.split("```json")[1].split("```")[0]
            elif "```" in cleaned:
                cleaned = cleaned.split("```")[1].split("```")[0]
            
            return json.loads(cleaned.strip())
        except:
            return {}
    
    def _fallback_metadata(self, file_info, df):
        """Template-based fallback if LLM fails"""
        return {
            "catalog_info": {
                "title": f"Government Dataset - {file_info['filename']}",
                "description": f"Harmonized dataset with {len(df)} records.",
                "sector": self.schema.get('data_standards', {}).get('sector', 'Unknown'),
                "keywords": ["government", "data", "india"]
            },
            "provenance": {
                "source": "Government of India",
                "publisher": "Unknown",
                "jurisdiction": "India"
            }
        }