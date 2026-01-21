import pandas as pd
import logging
import re

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StatsExtractor:
    """
    Extracts deterministic statistics from DataFrames to populate metadata.
    Avoids LLM hallucinations for factual data like dates and locations.
    """
    
    def extract_stats(self, df):
        """
        Main entry point: Analyzes dataframe for temporal, spatial, and quality stats.
        """
        stats = {
            'row_count': len(df),
            'col_count': len(df.columns),
            'temporal': self._get_temporal_range(df),
            'spatial': self._get_spatial_info(df),
            'quality': self._get_data_quality(df)
        }
        return stats

    def _get_temporal_range(self, df):
        """
        Finds min/max years from columns like 'year', 'date', 'fiscal_year'.
        """
        temporal = {
            'min_year': None,
            'max_year': None,
            'range_str': "Unknown"
        }
        
        # 1. Look for explicit 'year' column
        year_cols = [c for c in df.columns if 'year' in c.lower()]
        
        valid_years = []
        
        for col in year_cols:
            try:
                # Convert to numeric, forcing errors to NaNs
                numeric_vals = pd.to_numeric(df[col], errors='coerce').dropna()
                
                # Filter for reasonable year values (e.g., 1950 - 2030)
                # This avoids treating ID columns or counts as years
                valid = numeric_vals[(numeric_vals > 1900) & (numeric_vals < 2030)]
                
                if not valid.empty:
                    valid_years.extend(valid.tolist())
            except:
                continue
                
        if valid_years:
            min_y = int(min(valid_years))
            max_y = int(max(valid_years))
            temporal['min_year'] = min_y
            temporal['max_year'] = max_y
            
            if min_y == max_y:
                temporal['range_str'] = str(min_y)
            else:
                temporal['range_str'] = f"{min_y}-{max_y}"
                
        return temporal

    def _get_spatial_info(self, df):
        """
        Extracts unique districts/states to define spatial scope.
        """
        spatial = {
            'states': [],
            'districts': [],
            'granularity': 'Unknown'
        }
        
        # Identify spatial columns
        state_col = next((c for c in df.columns if 'state' in c.lower() and 'name' in c.lower()), None)
        if not state_col:
            # Try just 'state'
            state_col = next((c for c in df.columns if c.lower() == 'state'), None)
            
        dist_col = next((c for c in df.columns if 'district' in c.lower()), None)
        if not dist_col:
             dist_col = next((c for c in df.columns if c.lower() == 'dist'), None)

        # Extract values
        if state_col:
            states = df[state_col].dropna().astype(str).unique().tolist()
            # Clean list (remove 'Total', empty)
            states = [s for s in states if s.lower() != 'total' and len(s) > 1]
            spatial['states'] = sorted(states)[:5]  # Limit to 5 for metadata summary
            spatial['granularity'] = 'State'

        if dist_col:
            districts = df[dist_col].dropna().astype(str).unique().tolist()
            districts = [d for d in districts if d.lower() != 'total' and len(d) > 1]
            spatial['districts'] = sorted(districts)[:5]  # Limit
            spatial['granularity'] = 'District'
            
        return spatial

    def _get_data_quality(self, df):
        """
        Calculates basic quality metrics.
        """
        total_cells = df.size
        # Handling division by zero
        if total_cells == 0:
            return {'completeness_score': 0.0}
            
        missing_cells = df.isnull().sum().sum()
        completeness = 1.0 - (missing_cells / total_cells)
        
        return {
            'completeness_score': round(completeness, 2),
            'missing_cells': int(missing_cells),
            'total_rows': int(len(df))
        }
