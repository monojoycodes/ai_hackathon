import os
import json
from google import genai
from dotenv import load_dotenv

load_dotenv()

class UnifiedSchemaGenerator:
    """
    PHASE 1: Analyzes ALL files together and creates ONE unified schema.
    Enhanced to ensure EVERY column is mapped.
    """
    
    def __init__(self):
        self.client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))
    
    def generate_unified_schema(self, all_file_info):
        """
        Takes info from ALL files and generates ONE unified schema.
        
        Args:
            all_file_info: List of dicts with 'filename', 'columns', 'sample_data'
        
        Returns:
            Unified schema dict with standard column mappings
        """
        
        print("\n" + "="*70)
        print("🔍 PHASE 1: Analyzing ALL files to create unified schema...")
        print("="*70)
        
        # Extract ALL unique column names across all files
        all_columns = set()
        files_summary = {}
        
        for info in all_file_info:
            all_columns.update(info['columns'])
            files_summary[info['filename']] = {
                'columns': info['columns'],
                'sample': info['sample_data'][:500]  # Limit sample size
            }
        
        all_columns_list = sorted(list(all_columns))
        
        print(f"\n📊 Found {len(all_columns_list)} unique column names across {len(all_file_info)} files")
        print(f"All columns: {all_columns_list}\n")
        
        # Generate schema
        prompt = self._build_enhanced_prompt(files_summary, all_columns_list)
        
        try:
            response = self.client.models.generate_content(
                model="gemma-3-27b-it",
                contents=prompt
            )
            
            # Clean and parse response
            schema = self._parse_response(response.text)
            
            if schema:
                # Validate that all columns are mapped
                validation_result = self._validate_schema(schema, all_columns_list)
                
                if validation_result['valid']:
                    print(f"\n✅ Unified schema created successfully!")
                    print(f"   Domain: {schema.get('domain', 'Unknown')}")
                    print(f"   Standard columns: {list(schema.get('column_mappings', {}).keys())}")
                    print(f"   Coverage: {validation_result['mapped_count']}/{validation_result['total_count']} columns mapped")
                else:
                    print(f"\n⚠️  Schema created but some columns unmapped:")
                    print(f"   Missing: {validation_result['unmapped']}")
                    
                    # Try to fix missing columns
                    schema = self._auto_fix_missing_columns(schema, validation_result['unmapped'])
                
                return schema
            else:
                print("\n❌ Failed to parse LLM response")
                return self._create_fallback_schema(all_columns_list, files_summary)
            
        except Exception as e:
            print(f"❌ Schema generation failed: {e}")
            print("   Creating fallback schema...")
            return self._create_fallback_schema(all_columns_list, files_summary)
    
    def _build_enhanced_prompt(self, files_summary, all_columns_list):
        """
        Enhanced prompt that EXPLICITLY lists ALL columns and requires them to be mapped.
        """
        
        return f"""You are a Senior Data Architect for AIKosh (IndiaAI Mission).

CRITICAL TASK:
You MUST create a unified schema that maps EVERY SINGLE column name from ALL files.

==========================================
ALL UNIQUE COLUMN NAMES (MUST MAP ALL):
==========================================
{json.dumps(all_columns_list, indent=2)}

TOTAL COLUMNS TO MAP: {len(all_columns_list)}

==========================================
DETAILED FILE INFORMATION:
==========================================
{json.dumps(files_summary, indent=2)}

==========================================
YOUR TASK - FOLLOW THESE STEPS EXACTLY:
==========================================

STEP 1: IDENTIFY COLUMN GROUPS
Look at all {len(all_columns_list)} column names above and group similar ones together.

Example groupings:
- District columns: "district", "dist", "District_Name", "DISTRICT", "districtName", "zillakanaam", "Dist", "Dist_Code", "District Name", "Location"
- Cattle columns: "cattle", "Cattle_Count", "cattleCount", "gau_sankhya", "Total_Cattle", "Cattle", "TOTAL_CATTLE", "No of Cattle", "Bovine_Cattle", "Cat_Pop"
- State columns: "state", "State_Name", "stateName", "STATE", "rajyanaam", "St", "State Name", "Region"
- Buffalo columns: "buffalo", "Buffalo_Count", "buffaloCount", "buffaloes", "bhains_sankhya", "BUFALLO_POPULATION", "Buffalo", "No of Buffalo", "Bovine_Buffalo", "Buf_Pop"
- Goat columns: "goat", "Goat_Count", "goatCount", "goats", "bakri_sankhya", "GOAT_NOS", "Goat", "Goat Population", "Small_Ruminants_Goat", "Gt_Pop"
- Sheep columns: "sheep", "Sheep_Count", "sheepCount", "bhed_sankhya", "SHEEP_NOS", "Sheep", "Sheep Population", "Small_Ruminants_Sheep", "Shp_Pop"
- Year columns: "year", "Year", "yr", "Yr", "census_year", "surveyYear", "varsh", "YEAR", "Census_Year", "Data_Year"

STEP 2: CHOOSE STANDARD NAMES
For each group, pick ONE clear, professional snake_case name:
- Use lowercase with underscores
- Be consistent and professional
- Examples: "district", "state", "cattle_count", "buffalo_count", "goat_count", "sheep_count", "year"

STEP 3: CREATE COMPREHENSIVE MAPPINGS
List ALL variations for each standard column.

CRITICAL RULES:
✓ EVERY column from the list above MUST appear in exactly ONE mapping
✓ Include camelCase variations (e.g., "districtName", "cattleCount")
✓ Include snake_case variations (e.g., "district_name", "cattle_count")
✓ Include UPPERCASE variations (e.g., "DISTRICT", "TOTAL_CATTLE")
✓ Include spaces (e.g., "District Name", "No of Cattle")
✓ Include Hindi/transliterated names (e.g., "zillakanaam", "gau_sankhya")
✓ Include abbreviations (e.g., "dist", "yr", "Cat_Pop", "Buf_Pop")
✓ Include full names (e.g., "District_Name", "Census_Year")

==========================================
OUTPUT JSON STRUCTURE (STRICT):
==========================================
{{
    "domain": "Inferred domain (e.g., agriculture_livestock, water_resources, health_services)",
    
    "column_mappings": {{
        "district": [
            "district", "dist", "District", "DISTRICT", 
            "District_Name", "districtName", "District Name",
            "zillakanaam", "jilla", "Dist", "Dist_Code", "Location"
        ],
        "state": [
            "state", "State", "STATE", "State_Name", 
            "stateName", "State Name", "rajyanaam", "St", "Region"
        ],
        "cattle_count": [
            "cattle", "Cattle", "CATTLE", "Cattle_Count", "cattleCount",
            "gau_sankhya", "Total_Cattle", "TOTAL_CATTLE", 
            "No of Cattle", "Bovine_Cattle", "Cat_Pop"
        ],
        "buffalo_count": [
            "buffalo", "Buffalo", "BUFFALO", "Buffalo_Count", "buffaloCount",
            "buffaloes", "bhains_sankhya", "BUFALLO_POPULATION",
            "No of Buffalo", "Bovine_Buffalo", "Buf_Pop"
        ],
        "goat_count": [
            "goat", "Goat", "GOAT", "Goat_Count", "goatCount",
            "goats", "bakri_sankhya", "GOAT_NOS",
            "Goat Population", "Small_Ruminants_Goat", "Gt_Pop"
        ],
        "sheep_count": [
            "sheep", "Sheep", "SHEEP", "Sheep_Count", "sheepCount",
            "bhed_sankhya", "SHEEP_NOS",
            "Sheep Population", "Small_Ruminants_Sheep", "Shp_Pop"
        ],
        "year": [
            "year", "Year", "YEAR", "yr", "Yr", 
            "census_year", "surveyYear", "Census_Year",
            "varsh", "Data_Year"
        ],
        "survey_officer": [
            "Survey_Officer", "survey_officer", "officer"
        ]
    }},
    
    "column_types": {{
        "district": "string",
        "state": "string",
        "cattle_count": "integer",
        "buffalo_count": "integer",
        "goat_count": "integer",
        "sheep_count": "integer",
        "year": "integer",
        "survey_officer": "string"
    }},
    
    "required_columns": ["district", "year"],
    
    "data_standards": {{
        "jurisdiction_level": "District",
        "sector": "Livestock",
        "ministry": "Ministry of Fisheries, Animal Husbandry & Dairying"
    }}
}}

==========================================
VALIDATION CHECKLIST:
==========================================
Before returning, verify:
✓ All {len(all_columns_list)} column names are included in column_mappings
✓ No column appears in multiple standard columns
✓ Standard names use snake_case
✓ Data types are appropriate (string/integer/float)

==========================================
CRITICAL REMINDERS:
==========================================
- Map EVERY column from the list at the top
- One column can appear in only ONE standard column's variations
- Be thorough - include all naming patterns (camelCase, snake_case, UPPERCASE, spaces)
- Return ONLY valid JSON, no markdown backticks, no preamble

Return the JSON now:"""
    
    def _parse_response(self, response_text):
        """Clean and parse LLM response"""
        try:
            # Remove markdown formatting
            cleaned = response_text.strip()
            if "```json" in cleaned:
                cleaned = cleaned.split("```json")[1].split("```")[0]
            elif "```" in cleaned:
                cleaned = cleaned.split("```")[1].split("```")[0]
            
            schema = json.loads(cleaned.strip())
            return schema
            
        except Exception as e:
            print(f"⚠️  Failed to parse schema: {e}")
            print(f"Raw response (first 500 chars): {response_text[:500]}")
            return None
    
    def _validate_schema(self, schema, all_columns):
        """
        Validate that schema includes all columns.
        """
        column_mappings = schema.get('column_mappings', {})
        
        # Flatten all variations from schema
        mapped_columns = set()
        for standard_col, variations in column_mappings.items():
            mapped_columns.update(variations)
        
        # Find unmapped columns
        unmapped = [col for col in all_columns if col not in mapped_columns]
        
        return {
            'valid': len(unmapped) == 0,
            'mapped_count': len(all_columns) - len(unmapped),
            'total_count': len(all_columns),
            'unmapped': unmapped
        }
    
    def _auto_fix_missing_columns(self, schema, missing_columns):
        """
        Automatically add missing columns using fuzzy matching.
        """
        print(f"\n🔧 Auto-fixing {len(missing_columns)} unmapped columns...")
        
        from fuzzywuzzy import fuzz
        
        column_mappings = schema.get('column_mappings', {})
        
        for missing_col in missing_columns:
            # Normalize the missing column
            missing_normalized = missing_col.lower().replace('_', '').replace(' ', '')
            
            # Try to find best match among standard columns
            best_match = None
            best_score = 0
            
            for standard_col in column_mappings.keys():
                std_normalized = standard_col.lower().replace('_', '')
                score = fuzz.ratio(missing_normalized, std_normalized)
                
                if score > best_score:
                    best_score = score
                    best_match = standard_col
            
            # If good match found, add to that standard column
            if best_match and best_score > 70:
                column_mappings[best_match].append(missing_col)
                print(f"   ✓ Added '{missing_col}' → '{best_match}' (similarity: {best_score}%)")
            else:
                # Create new standard column for it
                new_standard = missing_col.lower().replace(' ', '_')
                column_mappings[new_standard] = [missing_col]
                schema['column_types'][new_standard] = 'string'
                print(f"   ⚠️  Created new standard column '{new_standard}' for '{missing_col}'")
        
        schema['column_mappings'] = column_mappings
        return schema
    
    def _create_fallback_schema(self, all_columns, files_summary):
        """
        Create a basic schema using pattern matching when LLM fails.
        """
        print("\n  Creating fallback schema using pattern matching...")
        
        schema = {
            'domain': 'unknown',
            'column_mappings': {},
            'column_types': {},
            'required_columns': [],
            'data_standards': {
                'jurisdiction_level': 'Unknown',
                'sector': 'Unknown',
                'ministry': 'Government of India'
            }
        }
        
        # Common patterns for grouping
        patterns = {
            'district': ['district', 'dist', 'zilla', 'jilla', 'location'],
            'state': ['state', 'rajya', 'region'],
            'cattle_count': ['cattle', 'gau', 'bovine.*cattle', 'cat'],
            'buffalo_count': ['buffalo', 'bhains', 'bovine.*buffalo', 'buf'],
            'goat_count': ['goat', 'bakri', 'ruminant.*goat', 'gt'],
            'sheep_count': ['sheep', 'bhed', 'ruminant.*sheep', 'shp'],
            'year': ['year', 'yr', 'varsh', 'data.*year', 'census.*year', 'survey.*year']
        }
        
        import re
        
        # Group columns by pattern
        for standard_col, pattern_list in patterns.items():
            variations = []
            
            for col in all_columns:
                col_lower = col.lower().replace('_', '').replace(' ', '')
                
                for pattern in pattern_list:
                    pattern_normalized = pattern.replace('_', '').replace(' ', '')
                    if re.search(pattern_normalized, col_lower):
                        variations.append(col)
                        break
            
            if variations:
                schema['column_mappings'][standard_col] = variations
                
                # Infer type
                if 'count' in standard_col or standard_col == 'year':
                    schema['column_types'][standard_col] = 'integer'
                else:
                    schema['column_types'][standard_col] = 'string'
        
        # Add any unmapped columns as-is
        mapped_cols = set()
        for variations in schema['column_mappings'].values():
            mapped_cols.update(variations)
        
        for col in all_columns:
            if col not in mapped_cols:
                col_standard = col.lower().replace(' ', '_')
                schema['column_mappings'][col_standard] = [col]
                schema['column_types'][col_standard] = 'string'
        
        print(f"   ✓ Created fallback schema with {len(schema['column_mappings'])} standard columns")
        
        return schema