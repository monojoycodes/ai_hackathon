# AIKosh Metadata Sentinel

## 🚀 Overview
AIKosh Metadata Sentinel is an intelligent data harmonization pipeline designed to transform India's fragmented Open Government Data (OGD) into an AI-ready national asset. It automates the ingestion, cleaning, and metadata enrichment of datasets, bridging the gap between raw data availability and actual usability.

## ✨ Features
- **Two-Phase Harmonization**: Analyzes all input files to generate a unified schema before transformation.
- **AI-Powered Metadata**: Uses Gemini models to generate rich, compliant metadata (Title, Description, Provenance).
- **Automated Cleaning**: Handles column normalization, data type inference, and deterministic stats extraction.
- **Portal & Local Ingestion**: Scrapes `data.gov.in` for context or processes local uploads.
- **Output Standardization**: Produces standardized CSVs and JSON metadata catalogs.

## 🛠️ Repository Structure
```
├── core/
│   ├── ingester.py          # File loading and encoding detection
│   ├── cleaner.py           # Data cleaning and normalization
│   ├── schema_generator.py  # Unified schema creation
│   ├── transformer.py       # Applies schema to datasets
│   ├── metadata_generator.py # Generates AI metadata
│   ├── portal_scraper.py    # Scrapes legacy portals
│   └── stats_extractor.py   # Deterministic statistics
├── outputs/                 # Harmonized data and metadata
├── uploads/                 # Raw input files
├── main.py                  # Main pipeline entry point
└── requirements.txt         # Dependencies
```

## ⚙️ Installation & Usage

### Prerequisites
- Python 3.8+
- Gemini API Key (set in `.env` as `GEMINI_API_KEY`)

### Setup
1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/aikosh-sentinel.git
   cd aikosh-sentinel
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Create a `.env` file:
   ```
   GEMINI_API_KEY=your_api_key_here
   ```

### Running the Pipeline
To run the harmonization process:
```bash
python main.py
```
Follow the interactive prompts to either scrape a URL or process local files in the `/uploads` directory.

## 🤝 Contributing
We welcome contributions to make government data more accessible!

1. **Fork** the repository.
2. **Clone** your fork locally.
3. **Create a branch** for your feature (`git checkout -b feature/amazing-feature`).
4. **Commit** your changes (`git commit -m 'Add amazing feature'`).
5. **Push** to the branch (`git push origin feature/amazing-feature`).
6. **Open a Pull Request**.

Please ensure you follow the existing code style and add tests for new functionality.

## 📄 License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
