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
    
    def generate_metadata(self, file_info, harmonized_df, transformation_log, scraped_metadata=None, stats=None):
        """
        Generate rich metadata for a harmonized file using Hybrid approach.
        Prioritizes Scraped Info > Deterministic Stats > LLM Inference.
        
        Args:
            file_info: Original file information
            harmonized_df: The harmonized DataFrame
            transformation_log: Dict with mapping info
            scraped_metadata: Dict with title, desc, ministry from Portal (Optional)
            stats: Dict with deterministic rows/cols/temporal/spatial (Optional)
        """
        
        # Build prompt with facts
        prompt = self._build_metadata_prompt(file_info, harmonized_df, transformation_log, scraped_metadata, stats)
        
        try:
            response = self.client.models.generate_content(
                model="gemma-3-27b-it",
                contents=prompt
            )
            
            metadata = self._parse_response(response.text)
            
            # --- ENFORCEMENT LAYER: Overwrite LLM hallucinations with specific facts ---
            
            # 1. Tech Metadata (Row counts, etc)
            metadata['technical_metadata'] = {
                'format': 'CSV',
                'original_filename': file_info['filename'],
                'row_count': len(harmonized_df),
                'column_count': len(harmonized_df.columns),
                'columns': harmonized_df.columns.tolist(),
                'data_quality': {
                    'standardized': True,
                    'ai_ready': True
                },
                'transformations_applied': transformation_log.get('mapping', {})
            }
            
            if stats:
                metadata['technical_metadata']['data_quality'].update(stats.get('quality', {}))
                
            # 2. Provenance (Use scraped if available)
            if scraped_metadata and scraped_metadata.get('ministry'):
                if 'provenance' not in metadata: metadata['provenance'] = {}
                metadata['provenance']['source'] = scraped_metadata['ministry']
                metadata['provenance']['publisher'] = "Government of India (via data.gov.in)"

             # 3. Spatial/Temporal (Use deterministic stats)
            if stats:
                if 'spatial_temporal' not in metadata: metadata['spatial_temporal'] = {}
                
                temp_range = stats.get('temporal', {}).get('range_str')
                if temp_range and temp_range != 'Unknown':
                    metadata['spatial_temporal']['temporal_coverage'] = temp_range
                    
                spatial = stats.get('spatial', {})
                if spatial.get('granularity') != 'Unknown':
                    metadata['spatial_temporal']['granularity'] = spatial['granularity']
                    # Add specific locations to description or keywords if not present
                    
            return metadata
            
        except Exception as e:
            print(f"   ⚠️  Metadata generation warning: {e}")
            return self._fallback_metadata(file_info, harmonized_df)
    
    def _build_metadata_prompt(self, file_info, df, transformation_log, scraped_metadata, stats):
        """Build AIKosh-compliant metadata prompt with FACTS"""
        
        # Prepare context strings
        scraped_context = "Not Available"
        if scraped_metadata:
            scraped_context = f"""
            - Official Title: {scraped_metadata.get('title')}
            - Official Description: {scraped_metadata.get('description')}
            - Ministry/Source: {scraped_metadata.get('ministry')}
            - Sector: {scraped_metadata.get('sector')}
            """
            
        stats_context = "Not Available"
        if stats:
            stats_context = f"""
            - Temporal Range (Years): {stats.get('temporal', {}).get('range_str')}
            - Spatial Granularity: {stats.get('spatial', {}).get('granularity')}
            - Sample Locations: {stats.get('spatial', {}).get('districts', [])} {stats.get('spatial', {}).get('states', [])}
            """
            
        return f"""You are a metadata expert for AIKosh (IndiaAI Mission).

FACTS (DO NOT HALLUCINATE THESE):
=========================================
OFFICIAL PORTAL CONTEXT:
{scraped_context}

DETERMINISTIC DATA STATS:
{stats_context}
=========================================

DATASET INFORMATION:
- Original filename: {file_info['filename']}
- Harmonized columns: {df.columns.tolist()}
- Sample data:
{df.head(2).to_string()}

YOUR TASK:
Generate AIKosh/IDMO-compliant metadata.
1. Use the OFFICIAL PORTAL TITLE if available. If not, generate a professional one.
2. Use the OFFICIAL DESCRIPTION as the base, and append a technical summary of the columns.
3. Use the DETERMINISTIC STATS for temporal/spatial coverage. Do not guess years if they are listed above.

OUTPUT JSON STRUCTURE (STRICT):
{{
    "catalog_info": {{
        "title": "Official Title or Professional Generated Title",
        "description": "Full description (Official + Technical)",
        "sector": "Sector (Agriculture/Health/Education/etc)",
        "keywords": ["keyword1", "keyword2"],
        "language": "English",
        "update_frequency": "Unknown"
    }},
    
    "provenance": {{
        "source": "Ministry Name (from context or inferred)",
        "publisher": "Department Name",
        "jurisdiction": "State/District",
        "data_owner": "Government of India"
    }},
    
    "spatial_temporal": {{
        "temporal_coverage": "Use exact years from STATS context",
        "spatial_coverage": "Geographic scope",
        "granularity": "District/State/Block"
    }},
    
    "usage": {{
        "license": "Open Government Data License - India",
        "use_cases": ["AI/ML training", "Policy analysis"],
        "target_audience": ["Researchers", "Data Scientists"]
    }}
}}
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
        """Template-based fallback"""
        return {
            "catalog_info": {
                "title": f"Government Dataset - {file_info['filename']}",
                "description": f"Harmonized dataset with {len(df)} records.",
                "sector": "Unknown"
            }
        }
