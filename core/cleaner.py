import pandas as pd
import numpy as np

class DataCleaner:
    def clean(self, df):
        """Performs structural cleaning on the DataFrame."""
        # 1. Drop completely empty rows and columns
        df = df.dropna(how='all').dropna(axis=1, how='all')
        
        # 2. Standardize column names (remove leading/trailing spaces)
        df.columns = [str(c).strip() for c in df.columns]
        
        # 3. Clean string values in cells
        df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
        
        # 4. Convert various 'null' indicators to actual NaN
        null_flags = ["NA", "na", "-", "Null", "NULL", ".", "None", ""]
        df.replace(null_flags, np.nan, inplace=True)
        
        return df