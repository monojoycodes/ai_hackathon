import pandas as pd
from fuzzywuzzy import fuzz

class SchemaTransformer:
    """
    PHASE 2: Applies the unified schema to individual files.
    Uses FUZZY MATCHING to handle variations the LLM might miss.
    """
    
    def __init__(self, unified_schema):
        self.schema = unified_schema
        self.column_mappings = unified_schema.get('column_mappings', {})
        self.column_types = unified_schema.get('column_types', {})
    
    def transform(self, df, filename):
        """
        Apply unified schema to transform a DataFrame.
        """
        
        print(f"\n📄 Transforming: {filename}")
        print(f"   Original columns: {df.columns.tolist()}")
        
        # Build mapping for this specific file
        column_mapping = self._build_file_mapping(df.columns)
        
        if not column_mapping:
            print(f"   ⚠️  WARNING: No columns could be mapped!")
            print(f"   This file will not be harmonized properly.")
        
        # Apply renaming
        df_transformed = df.rename(columns=column_mapping)
        
        print(f"   Mapped {len(column_mapping)}/{len(df.columns)} columns")
        
        # Convert data types
        df_transformed = self._convert_types(df_transformed)
        
        # Reorder columns to match standard order
        df_transformed = self._reorder_columns(df_transformed)
        
        print(f"   Final columns: {df_transformed.columns.tolist()}")
        
        return df_transformed, column_mapping
    
    def _build_file_mapping(self, file_columns):
        """
        Map this file's columns to standard schema.
        Uses THREE strategies:
        1. Exact match in variations
        2. Case-insensitive match
        3. Fuzzy matching (>85% similarity)
        """
        mapping = {}
        
        for original_col in file_columns:
            matched_standard = self._find_best_match(original_col)
            
            if matched_standard:
                mapping[original_col] = matched_standard
                print(f"      ✓ '{original_col}' → '{matched_standard}'")
            else:
                print(f"      ⚠️  Could not map: '{original_col}'")
        
        return mapping
    
    def _find_best_match(self, original_col):
        """
        Find which standard column this maps to.
        Uses multi-strategy matching.
        """
        original_lower = original_col.lower().strip()
        original_normalized = self._normalize_string(original_col)
        
        best_match = None
        best_score = 0
        
        for standard_col, variations in self.column_mappings.items():
            # Strategy 1: Exact match in variations list
            if original_col in variations:
                return standard_col
            
            # Strategy 2: Case-insensitive exact match
            if original_lower in [v.lower() for v in variations]:
                return standard_col
            
            # Strategy 3: Fuzzy matching against all variations
            for variation in variations:
                # Normalize both strings for comparison
                var_normalized = self._normalize_string(variation)
                
                # Calculate similarity
                score = fuzz.ratio(original_normalized, var_normalized)
                
                if score > best_score:
                    best_score = score
                    best_match = standard_col
        
        # Only return match if confidence is high enough
        if best_score >= 85:  # 85% similarity threshold
            return best_match
        
        # Strategy 4: Try matching against the standard column name itself
        standard_normalized = self._normalize_string(standard_col) if best_match else None
        if standard_normalized:
            score = fuzz.ratio(original_normalized, standard_normalized)
            if score >= 85:
                return best_match
        
        return None
    
    def _normalize_string(self, s):
        """
        Normalize string for better matching.
        Handles camelCase, snake_case, spaces, etc.
        """
        import re
        
        # Convert to lowercase
        s = s.lower().strip()
        
        # Remove special characters
        s = re.sub(r'[^\w\s]', '', s)
        
        # Replace spaces and underscores with nothing (for matching)
        s = s.replace('_', '').replace(' ', '')
        
        return s
    
    def _convert_types(self, df):
        """Convert columns to correct data types"""
        
        for col in df.columns:
            if col in self.column_types:
                target_type = self.column_types[col]
                
                try:
                    if target_type == 'integer':
                        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
                    elif target_type == 'float':
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                    elif target_type == 'string':
                        df[col] = df[col].astype(str)
                except Exception as e:
                    # Don't print warnings during transformation
                    pass
        
        return df
    
    def _reorder_columns(self, df):
        """Ensure consistent column order across all files"""
        
        # Get standard column order
        standard_order = list(self.column_mappings.keys())
        
        # Columns that exist in this file (in standard order)
        ordered_cols = [col for col in standard_order if col in df.columns]
        
        # Any extra columns not in standard schema
        extra_cols = [col for col in df.columns if col not in ordered_cols]
        
        # Return with standard columns first, then extras
        return df[ordered_cols + extra_cols]