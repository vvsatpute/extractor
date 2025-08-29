import requests
from bs4 import BeautifulSoup
import os
import json
from datetime import datetime
from urllib.parse import urlparse

# ------------------------------
# Config
# ------------------------------
OUTPUT_DIR = "extracted_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ------------------------------
# Fetcher
# ------------------------------
def fetch_html(url: str) -> str:
    try:
        headers = {"User-Agent": "CustomExtractor/1.0"}
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"[ERROR] Failed to fetch {url}: {e}")
        return ""

# ------------------------------
# Parser
# ------------------------------
def parse_html(url: str, html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    # Extract title
    title = soup.title.string.strip() if soup.title else None

    # Extract main text (basic: all <p> tags joined)
    paragraphs = [p.get_text(" ", strip=True) for p in soup.find_all("p")]
    main_text = "\n".join(paragraphs)

    # Build structured output
    return {
        "url": url,
        "timestamp": datetime.utcnow().isoformat(),
        "title": title,
        "main_text": main_text,
    }

# ------------------------------
# Loader
# ------------------------------
def save_to_json(data: dict):
    # Use domain name as folder
    domain = urlparse(data["url"]).netloc.replace(":", "_")
    domain_folder = os.path.join(OUTPUT_DIR, domain)
    os.makedirs(domain_folder, exist_ok=True)

    # Use timestamp as filename
    filename = os.path.join(
        domain_folder,
        f"{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
    )

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"[SAVED] {filename}")

# ------------------------------
# ETL Runner
# ------------------------------
def run_extraction(urls: list):
    for url in urls:
        print(f"[INFO] Processing: {url}")
        html = fetch_html(url)
        if html:
            parsed = parse_html(url, html)
            save_to_json(parsed)

# ------------------------------
# Example Run
# ------------------------------
if __name__ == "__main__":
    urls_to_extract = [
        "https://www.bbc.com/news",
    ]
    run_extraction(urls_to_extract)
