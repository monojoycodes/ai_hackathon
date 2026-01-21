import os
import time
import json
import logging
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PortalExtractor:
    """
    Extracts metadata and resources from Government Portals (e.g., data.gov.in).
    
    Capabilities:
    1. Search for datasets
    2. Scrape dataset landing pages for authoritative metadata (Title, Ministry, Desc)
    3. Download resource files (CSV, etc.)
    """
    
    BASE_URL = "https://data.gov.in"
    
    def __init__(self, output_dir="uploads"):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def extract_from_url(self, dataset_url):
        """
        Main entry point: Scrapes a dataset page and downloads valid resources.
        
        Returns:
            dict: {
                'metadata': dict,
                'resources': list_of_local_paths
            }
        """
        logger.info(f"🔍 Extracting from: {dataset_url}")
        
        try:
            response = self.session.get(dataset_url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 1. Scrape Metadata
            metadata = self._scrape_metadata(soup, dataset_url)
            
            # 2. Find and Download Resources
            resources = self._download_resources(soup, metadata['title'])
            
            return {
                'metadata': metadata,
                'resources': resources,
                'success': True
            }
            
        except Exception as e:
            logger.error(f"❌ Extraction failed for {dataset_url}: {e}")
            return {'success': False, 'error': str(e)}

    def _scrape_metadata(self, soup, url):
        """Scrape authoritative metadata from the page"""
        metadata = {
            'scraped_url': url,
            'scraped_timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
            'source_portal': 'data.gov.in'
        }
        
        # Title Strategy:
        # 1. Try specific data.gov.in ID 'title'
        # 2. Try OGD class 'og-node-title'
        # 3. Try standard h1
        title_elem = soup.find('h1', id='title') 
        if not title_elem:
             title_elem = soup.find('h1', class_='node-title')
        if not title_elem:
             title_elem = soup.find('h1', class_='title')
        
        if title_elem:
            metadata['title'] = title_elem.text.strip()
        else:
            # Fallback: Meta tags (OpenGraph) - Very reliable
            og_title = soup.find('meta', property='og:title')
            if og_title:
                metadata['title'] = og_title.get('content', '').strip()
            else:
                # Fallback: Page ID text lookups (Simpler text search)
                metadata['title'] = soup.title.string.split('|')[0].strip() if soup.title else 'Unknown Dataset'
        
        # Description
        desc_elem = soup.find('div', class_='field-name-body') or soup.find('div', class_='notes')
        if desc_elem:
            metadata['description'] = desc_elem.text.strip()
        else:
            # Fallback: Meta description
            og_desc = soup.find('meta', property='og:description') or soup.find('meta', attrs={'name': 'description'})
            metadata['description'] = og_desc.get('content', '').strip() if og_desc else ''
        
        # Organization / Ministry
        # Look for "Catalog" link or Organization field
        org_elem = soup.find('div', class_='og-group-ref')
        if org_elem:
            metadata['ministry'] = org_elem.text.strip()
        else:
            # Fallback: specific table checks
            metadata['ministry'] = self._find_text_by_label(soup, ["Ministry/Department", "Ministry", "Department", "Source"])
            
        # Sector
        metadata['sector'] = self._find_text_by_label(soup, ["Sector", "Category"])
        
        logger.info(f"   📋 Scraped Metadata: {metadata['title']} ({metadata.get('ministry', 'Unknown Data Owner')})")
        return metadata

    def _find_text_by_label(self, soup, label_texts):
        """Helper to find values in tables/lists by their label"""
        if isinstance(label_texts, str):
            label_texts = [label_texts]
            
        for text in label_texts:
            # Flexible search for label
            label = soup.find(lambda tag: tag.name in ['th', 'label', 'div', 'span'] and text in tag.text)
            if label:
                # Strategy 1: Table - Next sibling cell
                if label.name == 'th':
                    value_td = label.find_next_sibling('td')
                    if value_td: return value_td.text.strip()
                
                # Strategy 2: Div/Label - Next sibling div or span
                nxt = label.find_next_sibling()
                if nxt: return nxt.text.strip()
                
                # Strategy 3: Parent's next sibling (common in lists)
                if label.parent and label.parent.name in ['div', 'li']:
                    # Look for value inside the same parent (e.g. <div><label>Title:</label> <span>Value</span></div>)
                    value = label.parent.get_text().replace(text, '').replace(':', '').strip()
                    if len(value) > 2: return value

        return None

    def _download_resources(self, soup, dataset_title):
        """Find CSV/resource links and download them"""
        downloaded_paths = []
        
        # Find all resource links (looking for 'csv' or download/access buttons)
        # Targeted for data.gov.in structure (class 'resource-item' or 'data-format-csv')
        
        # Generic approach for common portals
        links = soup.find_all('a', href=True)
        csv_links = [l for l in links if 'csv' in l.get('href', '').lower() or 'csv' in l.text.lower()]
        
        # Filter duplicates
        unique_urls = set()
        for link in csv_links:
            href = link['href']
            # Make absolute URL
            if not href.startswith('http'):
                href = urljoin(self.BASE_URL, href)
                
            unique_urls.add(href)
            
        logger.info(f"   ⬇️  Found {len(unique_urls)} potential CSV resources")
        
        for i, url in enumerate(unique_urls):
            try:
                # Determine local filename
                # Clean title for filename
                safe_title = "".join([c for c in dataset_title if c.isalnum() or c in (' ', '-', '_')]).strip()
                safe_title = safe_title.replace(' ', '_').lower()[:50]
                
                filename = f"{safe_title}_{i+1}.csv"
                local_path = os.path.join(self.output_dir, filename)
                
                # Check if we already have it to avoid redownload (for testing)
                if os.path.exists(local_path):
                    logger.info(f"      Example file exists, skipping download: {filename}")
                    downloaded_paths.append(local_path)
                    continue

                # Download
                logger.info(f"      Downloading: {url}...")
                # Note: In a real hackathon, we might need verify=False or specific context
                with self.session.get(url, stream=True) as r:
                    r.raise_for_status()
                    with open(local_path, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            f.write(chunk)
                            
                logger.info(f"      ✅ Saved to: {local_path}")
                downloaded_paths.append(local_path)
                
                # Limit to 3 files per dataset for now
                if len(downloaded_paths) >= 3:
                    break
                    
            except Exception as e:
                logger.warning(f"      ⚠️  Download failed for {url}: {e}")
                
        return downloaded_paths

if __name__ == "__main__":
    # Test stub
    extractor = PortalExtractor()
    # Test with a sample URL if available or just print init
    print("Portal Extractor Initialized. Use functionality in main pipeline.")
