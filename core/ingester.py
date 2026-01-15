import pandas as pd
import os

class Ingester:
    def load_file(self, file_path):
        """Reads CSV or Excel files completely into memory."""
        ext = os.path.splitext(file_path)[1].lower()
        try:
            if ext == '.csv':
                return pd.read_csv(file_path)
            elif ext in ['.xlsx', '.xls']:
                return pd.read_excel(file_path)
            else:
                print(f"❌ Unsupported format: {ext}")
                return None
        except Exception as e:
            print(f"❌ Error reading {file_path}: {e}")
            return None