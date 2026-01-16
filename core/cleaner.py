import pandas as pd
import numpy as np
import re

class DataCleaner:
    """Enhanced cleaner for messy government data"""
    
    def clean(self, df):
        """Performs comprehensive cleaning on the DataFrame."""
        
        # 1. Drop completely empty rows and columns
        df = df.dropna(how='all').dropna(axis=1, how='all')
        
        # 2. Standardize column names
        df.columns = [self._clean_column_name(c) for c in df.columns]
        
        # 3. Clean string values in cells
        df = df.map(lambda x: self._clean_cell_value(x))
        
        # 4. Convert various 'null' indicators to actual NaN
        null_flags = ["NA", "na", "N/A", "n/a", "-", "Null", "NULL", 
                      "null", ".", "None", "none", "", " ", "  ",
                      "NaN", "nan", "#N/A", "#NA", "Missing", "missing"]
        df = df.replace(null_flags, np.nan)
        
        # 5. Remove rows that are entirely empty after cleaning
        df = df.dropna(how='all')
        
        # 6. Clean numeric columns
        df = self._clean_numeric_columns(df)
        
        return df
    
    def _clean_column_name(self, col_name):
        """Clean individual column name"""
        # Convert to string and strip
        col = str(col_name).strip()
        
        # Remove extra spaces
        col = ' '.join(col.split())
        
        return col
    
    def _clean_cell_value(self, value):
        """Clean individual cell value"""
        if pd.isna(value):
            return value
        
        if isinstance(value, str):
            # Strip whitespace
            value = value.strip()
            
            # Remove multiple spaces
            value = ' '.join(value.split())
            
            # Convert empty string to NaN
            if value == '' or value.isspace():
                return np.nan
            
            # Remove common garbage characters
            value = value.replace('\x00', '')  # Null bytes
            value = value.replace('\r', '')    # Carriage returns
            
        return value
    
    def _clean_numeric_columns(self, df):
        """
        Attempt to clean columns that should be numeric.
        Removes commas, currency symbols, etc.
        """
        for col in df.columns:
            # Check if column looks numeric
            if df[col].dtype == 'object':
                # Try to identify if it should be numeric
                sample = df[col].dropna().head(10)
                
                if len(sample) > 0:
                    # Check if most values look like numbers
                    numeric_count = 0
                    for val in sample:
                        if isinstance(val, str):
                            # Remove common numeric formatting
                            cleaned = val.replace(',', '').replace('$', '').replace('₹', '').strip()
                            try:
                                float(cleaned)
                                numeric_count += 1
                            except:
                                pass
                    
                    # If most values are numeric, clean the whole column
                    if numeric_count >= len(sample) * 0.7:
                        df[col] = df[col].apply(self._extract_number)
        
        return df
    
    def _extract_number(self, value):
        """Extract number from string"""
        if pd.isna(value):
            return value
        
        if isinstance(value, (int, float)):
            return value
        
        if isinstance(value, str):
            # Remove commas, currency symbols, spaces
            cleaned = value.replace(',', '').replace('$', '').replace('₹', '').replace(' ', '').strip()
            
            try:
                # Try to convert to number
                if '.' in cleaned:
                    return float(cleaned)
                else:
                    return int(cleaned)
            except:
                return value
        
        return value