import asyncio
import aiohttp
from bs4 import BeautifulSoup
from typing import List, Dict, Any
import json
import re
from datetime import datetime
import random
import time


class AsyncAmazonDealsExtractor:
    def __init__(self, urls: List[str]):
        self.urls = urls
        # Multiple User-Agent strings to rotate
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ]
        
        self.base_headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-US,en;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Cache-Control": "max-age=0",
            "DNT": "1",
            "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"'
        }

    def get_random_headers(self):
        """Get headers with a random User-Agent"""
        headers = self.base_headers.copy()
        headers["User-Agent"] = random.choice(self.user_agents)
        return headers

    async def fetch_with_retry(self, session: aiohttp.ClientSession, url: str, max_retries: int = 3) -> str:
        """Fetch HTML with retry logic and different strategies"""
        for attempt in range(max_retries):
            try:
                # Try different approaches
                if attempt == 0:
                    # First attempt: Normal request
                    headers = self.get_random_headers()
                    print(f"Attempt {attempt + 1}: Normal request with headers")
                elif attempt == 1:
                    # Second attempt: Add referer and different approach
                    headers = self.get_random_headers()
                    headers["Referer"] = "https://www.amazon.in/"
                    headers["Origin"] = "https://www.amazon.in"
                    print(f"Attempt {attempt + 1}: With referer and origin")
                else:
                    # Third attempt: Try mobile user agent
                    headers = self.get_random_headers()
                    headers["User-Agent"] = "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1.2 Mobile/15E148 Safari/604.1"
                    print(f"Attempt {attempt + 1}: Mobile user agent")
                
                # Add random delay between attempts
                if attempt > 0:
                    delay = random.uniform(2, 5)
                    print(f"Waiting {delay:.1f} seconds before retry...")
                    await asyncio.sleep(delay)
                
                async with session.get(url, headers=headers, timeout=30) as response:
                    print(f"Response status: {response.status}")
                    
                    if response.status == 200:
                        html = await response.text()
                        print(f"Successfully fetched HTML. Length: {len(html)} characters")
                        return html
                    elif response.status == 503:
                        print(f"503 Service Unavailable - Amazon is blocking requests")
                        if attempt < max_retries - 1:
                            continue
                    else:
                        print(f"HTTP {response.status}: {response.reason}")
                        if attempt < max_retries - 1:
                            continue
                        
            except asyncio.TimeoutError:
                print(f"Timeout on attempt {attempt + 1}")
                if attempt < max_retries - 1:
                    continue
            except Exception as e:
                print(f"Error on attempt {attempt + 1}: {str(e)}")
                if attempt < max_retries - 1:
                    continue
        
        print("All retry attempts failed")
        return ""

    async def fetch(self, session: aiohttp.ClientSession, url: str) -> str:
        """Main fetch method with retry logic"""
        return await self.fetch_with_retry(session, url)

    def extract_deals_from_page(self, html: str) -> List[Dict[str, Any]]:
        """Parse HTML and extract deals information from Amazon deals page"""
        soup = BeautifulSoup(html, "html.parser")
        deals = []
        
        print(f"Parsing HTML with BeautifulSoup...")
        
        # Debug: Check if we have any content
        if len(html) < 1000:
            print("Warning: HTML content seems too short, might be blocked or redirected")
            return deals
        
        # Try multiple selectors for deals - Amazon often changes their HTML structure
        deal_selectors = [
            # Modern Amazon selectors
            "div[data-component-type='s-deal-card']",
            "div[data-testid='deal-card']",
            "div[data-testid='product-card']",
            "div[data-asin]",
            
            # Alternative selectors
            ".a-section.a-spacing-base",
            ".a-section.a-spacing-none.a-spacing-top-mini",
            ".a-section.a-spacing-medium",
            
            # Grid-based selectors
            ".a-grid-row",
            ".a-row.a-size-base",
            
            # Product container selectors
            ".a-section.a-spacing-none.a-spacing-top-base",
            ".a-section.a-spacing-none.a-spacing-top-small",
            
            # Generic product selectors
            ".a-section[data-asin]",
            "div.a-section:has([data-asin])",
            
            # Fallback to any div with product-like content
            "div:has(a[href*='/dp/'])",
            "div:has(img[src*='images'])"
        ]
        
        deal_elements = []
        for i, selector in enumerate(deal_selectors):
            try:
                elements = soup.select(selector)
                print(f"Selector {i+1}: '{selector}' found {len(elements)} elements")
                if elements:
                    deal_elements = elements
                    print(f"Using selector: '{selector}' with {len(elements)} elements")
                    break
            except Exception as e:
                print(f"Selector {i+1} error: {str(e)}")
                continue
        
        if not deal_elements:
            print("No deal elements found. Trying alternative approach...")
            # Try to find any elements that might contain product information
            all_divs = soup.find_all('div')
            print(f"Total divs found: {len(all_divs)}")
            
            # Look for divs with potential product data
            for div in all_divs[:50]:  # Check first 50 divs
                if div.get('data-asin') or div.find('img') or div.find('a', href=re.compile(r'/dp/')):
                    deal_elements.append(div)
            
            print(f"Alternative approach found {len(deal_elements)} potential elements")
        
        if not deal_elements:
            print("Still no deal elements found. Amazon may have changed their structure significantly.")
            # Save HTML for debugging
            with open("debug_amazon_page.html", "w", encoding="utf-8") as f:
                f.write(html)
            print("Saved HTML to debug_amazon_page.html for inspection")
            return deals
        
        print(f"Processing {len(deal_elements)} deal elements...")
        
        for i, deal_element in enumerate(deal_elements[:20]):  # Limit to first 20 for debugging
            try:
                deal_info = self.extract_single_deal(deal_element)
                if deal_info and deal_info.get('title'):  # Only add if we have at least a title
                    deals.append(deal_info)
                    print(f"Extracted deal {i+1}: {deal_info.get('title', 'No title')[:50]}...")
                else:
                    print(f"Deal {i+1}: No useful data extracted")
            except Exception as e:
                print(f"Error extracting deal {i+1}: {str(e)}")
                continue
        
        return deals

    def extract_single_deal(self, deal_element) -> Dict[str, Any]:
        """Extract information from a single deal element"""
        deal_info = {}
        
        # Product Title - Multiple selector strategies
        title_selectors = [
            "h2 a span",
            "h2 span",
            ".a-size-base-plus.a-color-base.a-text-normal",
            ".a-size-medium.a-color-base.a-text-normal",
            ".a-size-small.a-color-base.a-text-normal",
            "a[data-testid='deal-card-link'] span",
            ".a-link-normal.a-text-normal",
            ".a-link-normal span",
            "span.a-text-normal",
            "a span.a-text-normal",
            ".a-size-base-plus",
            ".a-size-medium",
            ".a-size-small"
        ]
        
        for selector in title_selectors:
            title_tag = deal_element.select_one(selector)
            if title_tag:
                title_text = title_tag.get_text(strip=True)
                if title_text and len(title_text) > 5:  # Ensure it's a meaningful title
                    deal_info["title"] = title_text
                    break
        
        # Product Link
        link_selectors = [
            "h2 a",
            "a[data-testid='deal-card-link']",
            ".a-link-normal.a-text-normal",
            "a[href*='/dp/']",
            "a[href*='/gp/product/']",
            "a:has(img)"
        ]
        
        for selector in link_selectors:
            link_tag = deal_element.select_one(selector)
            if link_tag and link_tag.get("href"):
                href = link_tag["href"]
                if href.startswith("/"):
                    href = "https://www.amazon.in" + href
                deal_info["product_link"] = href
                break
        
        # Price Information
        price_selectors = [
            ".a-price .a-offscreen",
            ".a-price-whole",
            ".a-price-current .a-offscreen",
            ".a-price .a-price-whole",
            ".a-price-current .a-price-whole",
            "span.a-price-whole",
            ".a-price-range .a-offscreen"
        ]
        
        for selector in price_selectors:
            price_tag = deal_element.select_one(selector)
            if price_tag:
                price_text = price_tag.get_text(strip=True)
                if price_text and any(char.isdigit() for char in price_text):
                    deal_info["price"] = price_text
                    break
        
        # Original Price (MRP)
        mrp_selectors = [
            ".a-price.a-text-price .a-offscreen",
            ".a-text-strike",
            ".a-price.a-text-price .a-price-whole",
            ".a-price-range .a-text-price .a-offscreen"
        ]
        
        for selector in mrp_selectors:
            mrp_tag = deal_element.select_one(selector)
            if mrp_tag:
                mrp_text = mrp_tag.get_text(strip=True)
                if mrp_text and any(char.isdigit() for char in mrp_text):
                    deal_info["original_price"] = mrp_text
                    break
        
        # Discount Percentage
        discount_selectors = [
            ".a-size-base.a-color-price",
            ".a-badge-text",
            ".a-color-secondary",
            ".a-size-base-plus.a-color-price",
            ".a-size-small.a-color-price"
        ]
        
        for selector in discount_selectors:
            discount_tag = deal_element.select_one(selector)
            if discount_tag:
                discount_text = discount_tag.get_text(strip=True)
                if discount_text:
                    # Extract percentage from text like "Up to 40% off"
                    percentage_match = re.search(r'(\d+)%', discount_text)
                    if percentage_match:
                        deal_info["discount_percentage"] = percentage_match.group(1) + "%"
                    deal_info["discount_text"] = discount_text
                    break
        
        # Calculate discount percentage from price difference if not found
        if not deal_info.get("discount_percentage") and deal_info.get("price") and deal_info.get("original_price"):
            try:
                # Clean price strings and convert to numbers
                current_price = deal_info["price"].replace("₹", "").replace(",", "").strip()
                original_price = deal_info["original_price"].replace("₹", "").replace(",", "").strip()
                
                if current_price.replace(".", "").isdigit() and original_price.replace(".", "").isdigit():
                    current_val = float(current_price)
                    original_val = float(original_price)
                    
                    if original_val > current_val:
                        discount_percent = round(((original_val - current_val) / original_val) * 100)
                        deal_info["calculated_discount"] = f"{discount_percent}%"
                        deal_info["savings"] = f"₹{original_val - current_val:.0f}"
            except (ValueError, ZeroDivisionError):
                pass
        
        # Product Image
        img_selectors = [
            "img[data-src]",
            "img[src]",
            ".a-image-container img",
            "img.a-dynamic-image",
            "img[alt*='product']"
        ]
        
        for selector in img_selectors:
            img_tag = deal_element.select_one(selector)
            if img_tag:
                img_src = img_tag.get("data-src") or img_tag.get("src")
                if img_src and not img_src.endswith('.gif'):  # Skip loading gifs
                    deal_info["image_url"] = img_src
                    break
        
        # ASIN (Amazon Standard Identification Number)
        asin = deal_element.get("data-asin")
        if asin:
            deal_info["asin"] = asin
        
        # Rating
        rating_selectors = [
            ".a-icon-alt",
            ".a-size-base.a-color-secondary",
            "span[aria-label*='stars']",
            ".a-icon-star"
        ]
        
        for selector in rating_selectors:
            rating_tag = deal_element.select_one(selector)
            if rating_tag:
                rating_text = rating_tag.get_text(strip=True)
                rating_match = re.search(r'(\d+\.?\d*)', rating_text)
                if rating_match:
                    deal_info["rating"] = rating_match.group(1)
                    break
        
        # Prime Badge
        prime_selectors = [
            ".a-icon-prime",
            ".a-icon-prime.a-icon-small",
            "span[aria-label*='Prime']"
        ]
        
        for selector in prime_selectors:
            prime_tag = deal_element.select_one(selector)
            if prime_tag:
                deal_info["prime_eligible"] = True
                break
        
        # Deal Badge
        deal_badge_selectors = [
            ".a-badge-text",
            ".a-color-secondary",
            ".a-size-base-plus.a-color-price",
            ".a-size-small.a-color-price"
        ]
        
        for selector in deal_badge_selectors:
            badge_tag = deal_element.select_one(selector)
            if badge_tag:
                badge_text = badge_tag.get_text(strip=True)
                if badge_text and any(keyword in badge_text.lower() for keyword in ["deal", "off", "save", "discount", "%"]):
                    deal_info["deal_badge"] = badge_text
                    break
        
        return deal_info if deal_info else None

    async def process_url(self, session: aiohttp.ClientSession, url: str) -> Dict[str, Any]:
        """Fetch + parse for one URL"""
        html = await self.fetch(session, url)
        if html:
            deals = self.extract_deals_from_page(html)
            return {
                "url": url,
                "timestamp": datetime.now().isoformat(),
                "total_deals": len(deals),
                "deals": deals
            }
        return {"url": url, "error": "Failed to fetch HTML"}

    async def run(self) -> List[Dict[str, Any]]:
        """Run the extractor for all URLs"""
        # Configure session with better settings
        connector = aiohttp.TCPConnector(
            limit=5,  # Reduce concurrent connections
            limit_per_host=2,  # Reduce per-host connections
            ttl_dns_cache=300,  # Cache DNS results
            use_dns_cache=True
        )
        timeout = aiohttp.ClientTimeout(total=60, connect=20)
        
        async with aiohttp.ClientSession(
            connector=connector, 
            timeout=timeout,
            cookie_jar=aiohttp.CookieJar()
        ) as session:
            tasks = [self.process_url(session, url) for url in self.urls]
            return await asyncio.gather(*tasks, return_exceptions=True)

    def save_results(self, results: List[Dict[str, Any]], filename: str = None):
        """Save results to JSON file"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"amazon_deals_{timestamp}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        
        print(f"Results saved to {filename}")


# Example usage
if __name__ == "__main__":
    # Try different Amazon URLs that might be less protected
    urls = [
        "https://www.amazon.in/deals?ref_=nav_cs_gb",
        "https://www.amazon.in/gp/goldbox?ref_=nav_cs_gb",  # Alternative deals URL
        "https://www.amazon.in/s?i=specialty-aps&bbn=1389401031&rh=n%3A1389401031%2Cn%3A1389402031&ref=nav_em__nav_desktop_sa_intl_computers_0_2_1_2"  # Electronics deals
    ]

    extractor = AsyncAmazonDealsExtractor(urls)
    
    try:
        print("Starting Amazon deals extraction with anti-bot bypass...")
        results = asyncio.run(extractor.run())
        
        # Process results
        for result in results:
            if isinstance(result, Exception):
                print(f"Error processing URL: {result}")
                continue
                
            if "error" in result:
                print(f"Error for {result['url']}: {result['error']}")
                continue
            
            print(f"\nURL: {result['url']}")
            print(f"Total deals found: {result['total_deals']}")
            print(f"Timestamp: {result['timestamp']}")
            
            # Save results
            extractor.save_results([result])
            
    except Exception as e:
        print(f"Error running extractor: {str(e)}")
