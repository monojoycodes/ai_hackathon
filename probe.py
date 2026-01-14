import asyncio
from playwright.async_api import async_playwright

async def scrape_portal(url):
    async with async_playwright() as p:
        # headless=False helps you see what's happening during debugging
        browser = await p.chromium.launch(headless=False) 
        page = await browser.new_page()
        
        print(f"Navigating to: {url}")
        # Setting a longer timeout for slow govt servers
        await page.goto(url, wait_until="networkidle", timeout=60000)

        # Wait for the catalog cards to appear
        # The titles are usually inside <h3> or <a> tags within a specific div
        await page.wait_for_selector(".view-content", timeout=10000)
        
        # New approach: Get all links that look like catalog entries
        # We target the specific 'Farmer Suicide' or 'Grievance Data' titles
        cards = await page.query_selector_all(".views-row")
        
        raw_results = []
        for card in cards[:5]:
            # Extract title
            title_el = await card.query_selector("h3 a")
            if title_el:
                title = await title_el.inner_text()
                link = await title_el.get_attribute("href")
                
                # Extract meta info (Organization/Department)
                meta_el = await card.query_selector(".views-field-field-organization-name")
                meta_text = await meta_el.inner_text() if meta_el else "N/A"
                
                raw_results.append({
                    "title": title.strip(),
                    "link": f"https://data.gov.in{link}",
                    "raw_meta": meta_text.strip()
                })
        
        await browser.close()
        return raw_results

if __name__ == "__main__":
    target = "https://www.data.gov.in/catalogs"
    data = asyncio.run(scrape_portal(target))
    import json
    print(json.dumps(data, indent=2))