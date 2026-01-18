import os
import json
import pandas as pd
from google import genai
from dotenv import load_dotenv
from core.logging_utils import get_logger

load_dotenv()
logger = get_logger(__name__)

class MetadataGenerator:
    """
    Generates AIKosh/IDMO-compliant metadata for harmonized datasets.
    """
    MAX_LOCATION_PREVIEW = 5
    NULL_PLACEHOLDERS = {'nan', 'none', 'null'}
    
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
            
            metadata = self._parse_response(response.text) or {}
            
            temporal_coverage = self._extract_temporal_coverage(harmonized_df)
            spatial_summary = self._extract_spatial_coverage(harmonized_df)
            data_completeness = self._calculate_completeness(harmonized_df)
            schema_coverage = self._calculate_schema_coverage(harmonized_df, transformation_log)
            
            metadata.setdefault('spatial_temporal', {})
            if temporal_coverage:
                metadata['spatial_temporal']['temporal_coverage'] = temporal_coverage
            if spatial_summary.get('spatial_coverage'):
                metadata['spatial_temporal']['spatial_coverage'] = spatial_summary['spatial_coverage']
            if spatial_summary.get('geotags'):
                metadata['spatial_temporal']['geotags'] = spatial_summary['geotags']
            
            metadata['technical_metadata'] = {
                'format': file_info.get('format', 'CSV'),
                'original_filename': file_info['filename'],
                'row_count': len(harmonized_df),
                'column_count': len(harmonized_df.columns),
                'columns': harmonized_df.columns.tolist(),
                'data_quality': {
                    'completeness': data_completeness,
                    'standardized': True,
                    'ai_ready': True
                },
                'transformations_applied': transformation_log.get('mapping', {})
            }

            metadata['quality_metrics'] = self._calculate_quality_metrics(
                metadata,
                data_completeness,
                schema_coverage
            )
            
            return metadata
            
        except Exception as e:
            logger.exception("Metadata generation failed for %s: %s", file_info.get('filename'), e)
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
        except Exception as e:
            logger.warning("Failed to parse metadata JSON: %s", e)
            return {}

    def _calculate_completeness(self, df):
        if df.empty or len(df.columns) == 0:
            return 0.0
        return round(
            1 - (df.isnull().sum().sum() / (len(df) * len(df.columns))),
            2
        )

    def _calculate_schema_coverage(self, df, transformation_log):
        if df.empty:
            return 0.0
        mapped = transformation_log.get('mapping', {})
        if len(df.columns) == 0 or not mapped:
            return 0.0
        coverage = len(mapped) / max(len(df.columns), 1)
        return round(min(coverage, 1.0), 2)

    def _extract_temporal_coverage(self, df):
        temporal_cols = [
            col for col in df.columns
            if any(token in col.lower() for token in ['year', 'date', 'time'])
        ]
        for col in temporal_cols:
            series = df[col].dropna()
            if series.empty:
                continue
            if pd.api.types.is_numeric_dtype(series):
                years = pd.to_numeric(series, errors='coerce').dropna()
                if not years.empty:
                    if years.apply(self._is_integer_year).all():
                        min_year = int(years.min())
                        max_year = int(years.max())
                        return str(min_year) if min_year == max_year else f"{min_year}-{max_year}"
                    min_year = round(float(years.min()), 2)
                    max_year = round(float(years.max()), 2)
                    return str(min_year) if min_year == max_year else f"{min_year}-{max_year}"
            parsed = pd.to_datetime(series, errors='coerce')
            parsed = parsed.dropna()
            if not parsed.empty:
                min_date = parsed.min().date().isoformat()
                max_date = parsed.max().date().isoformat()
                return min_date if min_date == max_date else f"{min_date} to {max_date}"
        return None

    def _is_integer_year(self, value):
        try:
            if isinstance(value, bool):
                return False
            if isinstance(value, int):
                return True
            if isinstance(value, float):
                return value.is_integer()
            if hasattr(value, 'is_integer'):
                return value.is_integer()
            return float(value).is_integer()
        except (TypeError, ValueError, OverflowError):
            return False

    def _extract_spatial_coverage(self, df):
        spatial = {}
        location_values = []
        lower_columns = {col: col.lower() for col in df.columns}
        location_cols = [
            col for col, lower_col in lower_columns.items()
            if any(token in lower_col for token in ['state', 'district', 'region'])
        ]
        for col in location_cols:
            location_values.extend(df[col].dropna().astype(str).tolist())

        normalized_locations = self._normalize_geo_values(location_values)
        if normalized_locations:
            spatial['spatial_coverage'] = (
                ", ".join(normalized_locations[:self.MAX_LOCATION_PREVIEW])
                if len(normalized_locations) <= self.MAX_LOCATION_PREVIEW
                else "Multiple locations"
            )

        lat_col = self._find_geo_column(df.columns, ['lat', 'latitude'])
        lon_col = self._find_geo_column(df.columns, ['lon', 'lng', 'longitude'])
        if lat_col and lon_col:
            lat = pd.to_numeric(df[lat_col], errors='coerce')
            lon = pd.to_numeric(df[lon_col], errors='coerce')
            lat_clean = lat.dropna()
            lon_clean = lon.dropna()
            if not lat_clean.empty and not lon_clean.empty:
                lat_min = float(round(lat_clean.min(), 6))
                lat_max = float(round(lat_clean.max(), 6))
                lon_min = float(round(lon_clean.min(), 6))
                lon_max = float(round(lon_clean.max(), 6))
                spatial['geotags'] = {
                    'latitude_range': [lat_min, lat_max],
                    'longitude_range': [lon_min, lon_max]
                }

        return spatial

    def _find_geo_column(self, columns, keywords):
        for col in columns:
            lower_col = col.lower()
            if any(keyword in lower_col for keyword in keywords):
                return col
        return None

    def _normalize_geo_values(self, values):
        normalized = set()
        for value in values:
            text = str(value).strip()
            if not text or text.lower() in self.NULL_PLACEHOLDERS:
                continue
            normalized.add(text.title())
        return sorted(normalized)

    def _calculate_quality_metrics(self, metadata, data_completeness, schema_coverage):
        required_fields = [
            ('catalog_info', 'title'),
            ('catalog_info', 'description'),
            ('catalog_info', 'sector'),
            ('provenance', 'source'),
            ('spatial_temporal', 'temporal_coverage'),
            ('spatial_temporal', 'spatial_coverage')
        ]

        filled = 0
        for section, field in required_fields:
            value = metadata.get(section, {}).get(field)
            if value:
                filled += 1

        metadata_completeness = round(filled / len(required_fields), 2)
        quality_score = round(
            (metadata_completeness + data_completeness + schema_coverage) / 3,
            2
        )

        return {
            'metadata_completeness': metadata_completeness,
            'data_completeness': data_completeness,
            'schema_coverage': schema_coverage,
            'quality_score': quality_score
        }
    
    def _fallback_metadata(self, file_info, df):
        """Template-based fallback if LLM fails"""
        data_completeness = self._calculate_completeness(df)
        temporal_coverage = self._extract_temporal_coverage(df)
        spatial_summary = self._extract_spatial_coverage(df)
        fallback = {
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
        fallback['spatial_temporal'] = {}
        if temporal_coverage:
            fallback['spatial_temporal']['temporal_coverage'] = temporal_coverage
        if spatial_summary.get('spatial_coverage'):
            fallback['spatial_temporal']['spatial_coverage'] = spatial_summary['spatial_coverage']
        if spatial_summary.get('geotags'):
            fallback['spatial_temporal']['geotags'] = spatial_summary['geotags']
        fallback['technical_metadata'] = {
            'format': file_info.get('format', 'CSV'),
            'original_filename': file_info['filename'],
            'row_count': len(df),
            'column_count': len(df.columns),
            'columns': df.columns.tolist(),
            'data_quality': {
                'completeness': data_completeness,
                'standardized': True,
                'ai_ready': True
            },
            'transformations_applied': {}
        }
        fallback['quality_metrics'] = self._calculate_quality_metrics(
            fallback,
            data_completeness,
            0.0
        )
        return fallback
