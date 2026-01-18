import pandas as pd
import os

class Ingester:
    """Loads CSV, Excel, TSV, JSON, and JSONL files into memory"""
    
    def load_file(self, file_path):
        """Reads CSV or Excel files completely into memory."""
        ext = os.path.splitext(file_path)[1].lower()
        
        try:
            if ext == '.csv':
                df = pd.read_csv(file_path, encoding='utf-8', on_bad_lines='skip')
            elif ext == '.tsv':
                df = pd.read_csv(file_path, sep='\t', encoding='utf-8', on_bad_lines='skip')
            elif ext in ['.xlsx', '.xls']:
                df = pd.read_excel(file_path)
            elif ext == '.json':
                df = pd.read_json(file_path)
            elif ext == '.jsonl':
                df = pd.read_json(file_path, lines=True)
            else:
                print(f"❌ Unsupported format: {ext}")
                return None
            
            return df
            
        except Exception as e:
            print(f"❌ Error reading {file_path}: {e}")
            return None
    
    def get_file_info(self, file_path, df):
        """Extract file metadata for analysis"""
        ext = os.path.splitext(file_path)[1].lower()
        return {
            'filename': os.path.basename(file_path),
            'filepath': file_path,
            'format': ext.lstrip('.').upper() if ext else 'UNKNOWN',
            'columns': df.columns.tolist(),
            'row_count': len(df),
            'sample_data': df.head(3).to_string()
        }
