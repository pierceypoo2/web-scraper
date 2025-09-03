import os
import json
import requests
import re
import time
import random
from typing import List, Dict, Optional
from datetime import datetime
from bs4 import BeautifulSoup
from pydantic import BaseModel
from urllib.parse import urljoin, urlparse
import urllib3

# Disable SSL warnings for problematic sites
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class Entity(BaseModel):
    name: str
    description: str

class Relationship(BaseModel):
    entity1: Entity
    entity2: Entity
    description: str
    relation_type: str

class KnowledgeGraph(BaseModel):
    entities: List[Entity]
    relationships: List[Relationship]

class EnhancedWebScraper:
    def __init__(self):
        self.session = requests.Session()
        
        # Rotate through different user agents
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15'
        ]
        
        # Free proxy rotation (GitHub Actions has different IPs each run anyway)
        self.free_proxies = []
        self.current_proxy_index = 0
        self.load_free_proxies()
        
        # Common headers to appear more human
        self.headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        }
        
        self.session.headers.update(self.headers)

    def load_free_proxies(self):
        """Load free proxy servers for IP rotation"""
        print("🌐 Loading free proxy servers...")
        
        # Free proxy APIs (these change regularly)
        free_proxy_sources = [
            'https://www.proxy-list.download/api/v1/get?type=http&anon=elite&country=US',
            'https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt',
            'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt'
        ]
        
        for source in free_proxy_sources:
            try:
                response = requests.get(source, timeout=10)
                if response.status_code == 200:
                    # Parse proxy list (format: IP:PORT)
                    proxies = re.findall(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{2,5}', response.text)
                    self.free_proxies.extend(proxies[:5])  # Take first 5 from each source
                    
            except Exception as e:
                print(f"   Failed to load proxies from {source}: {e}")
                continue
        
        # Remove duplicates
        self.free_proxies = list(set(self.free_proxies))[:15]  # Keep max 15 proxies
        
        if self.free_proxies:
            print(f"   ✅ Loaded {len(self.free_proxies)} proxy servers")
        else:
            print("   ⚠️ No proxies loaded - using direct connection")

    def get_next_proxy(self):
        """Get the next proxy in rotation"""
        if not self.free_proxies:
            return None
            
        proxy = self.free_proxies[self.current_proxy_index]
        self.current_proxy_index = (self.current_proxy_index + 1) % len(self.free_proxies)
        
        return {
            'http': f'http://{proxy}',
            'https': f'http://{proxy}'
        }

    def test_proxy(self, proxy_dict):
        """Test if a proxy is working"""
        try:
            test_response = requests.get(
                'https://httpbin.org/ip', 
                proxies=proxy_dict, 
                timeout=5,
                headers={'User-Agent': random.choice(self.user_agents)}
            )
            if test_response.status_code == 200:
                ip_data = test_response.json()
                print(f"   ✅ Proxy working - IP: {ip_data.get('origin', 'unknown')}")
                return True
        except:
            pass
        return False
        
    def get_random_headers(self):
        """Get randomized headers for each request"""
        headers = self.headers.copy()
        headers['User-Agent'] = random.choice(self.user_agents)
        return headers

    def scrape_url(self, url: str, max_retries: int = 5) -> Dict:
        """Enhanced scraping with IP rotation and anti-detection measures"""
        print(f"🔍 Enhanced scraping: {url}")
        
        for attempt in range(max_retries):
            try:
                # Random delay between attempts
                if attempt > 0:
                    delay = random.uniform(3, 8)
                    print(f"   Retry {attempt + 1}/{max_retries} after {delay:.1f}s delay...")
                    time.sleep(delay)
                
                # Get random headers and proxy for each attempt
                headers = self.get_random_headers()
                proxy_dict = self.get_next_proxy()
                
                # Show which IP we're using
                if proxy_dict:
                    proxy_ip = proxy_dict['http'].split('@')[-1] if '@' in proxy_dict['http'] else proxy_dict['http'].replace('http://', '')
                    print(f"   🌐 Using proxy: {proxy_ip}")
                    
                    # Test proxy first (only on first few attempts)
                    if attempt < 2 and not self.test_proxy(proxy_dict):
                        print(f"   ❌ Proxy failed, trying next...")
                        continue
                else:
                    print(f"   🌐 Using direct connection (GitHub runner IP)")
                
                # Try different request strategies with proxy
                if 'zillow' in url.lower():
                    knowledge = self.scrape_zillow_specific(url, headers, proxy_dict)
                elif 'linkedin' in url.lower():
                    knowledge = self.scrape_linkedin_specific(url, headers, proxy_dict)
                elif 'amazon' in url.lower():
                    knowledge = self.scrape_amazon_specific(url, headers, proxy_dict)
                else:
                    knowledge = self.scrape_generic_enhanced(url, headers, proxy_dict)
                
                if not knowledge.get('error'):
                    knowledge['source_url'] = url
                    knowledge['scraping_method'] = 'enhanced_with_proxy' if proxy_dict else 'enhanced_direct'
                    knowledge['proxy_used'] = proxy_dict is not None
                    return knowledge
                    
            except Exception as e:
                print(f"   Attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    return {
                        "error": True, 
                        "message": f"Failed after {max_retries} attempts: {str(e)}", 
                        "source_url": url
                    }
        
        return {"error": True, "message": "Max retries exceeded", "source_url": url}

    def scrape_zillow_specific(self, url: str, headers: Dict, proxy_dict: Dict = None) -> Dict:
        """Zillow-specific scraping strategy with proxy support"""
        print("   🏠 Using Zillow-specific strategy with IP rotation...")
        
        # Add Zillow-specific headers
        zillow_headers = headers.copy()
        zillow_headers.update({
            'Referer': 'https://www.google.com/',
            'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"'
        })
        
        response = self.session.get(
            url, 
            headers=zillow_headers, 
            proxies=proxy_dict,
            timeout=20,
            verify=False,  # Skip SSL verification if needed
            allow_redirects=True
        )
        
        response.raise_for_status()
        
        # Parse with more aggressive content extraction
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Remove problematic elements
        for element in soup(['script', 'style', 'nav', 'footer', 'header', 'aside', 'noscript']):
            element.decompose()
        
        # Extract text more aggressively
        text_parts = []
        
        # Try to find main content areas
        main_content = soup.find('main') or soup.find('div', class_='main') or soup.find('body')
        
        if main_content:
            # Get all text from paragraphs, divs, spans
            for element in main_content.find_all(['p', 'div', 'span', 'h1', 'h2', 'h3', 'li']):
                text = element.get_text(strip=True)
                if text and len(text) > 10:  # Only meaningful text
                    text_parts.append(text)
        
        # If we didn't get much, try all text
        if len(text_parts) < 5:
            text_parts = [soup.get_text()]
        
        combined_text = ' '.join(text_parts)
        
        return self.extract_knowledge_from_text(combined_text[:10000], url, method="zillow_specific_proxy")

    def scrape_linkedin_specific(self, url: str, headers: Dict, proxy_dict: Dict = None) -> Dict:
        """LinkedIn-specific scraping strategy with proxy support"""
        print("   💼 Using LinkedIn-specific strategy with IP rotation...")
        
        linkedin_headers = headers.copy()
        linkedin_headers.update({
            'Referer': 'https://www.google.com/',
            'X-Requested-With': 'XMLHttpRequest'
        })
        
        response = self.session.get(url, headers=linkedin_headers, proxies=proxy_dict, timeout=20)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # LinkedIn-specific content extraction
        content_areas = soup.find_all(['div', 'section'], class_=re.compile(r'(content|profile|experience|education)'))
        
        text = ' '.join([area.get_text(strip=True) for area in content_areas if area.get_text(strip=True)])
        
        return self.extract_knowledge_from_text(text[:8000], url, method="linkedin_specific_proxy")

    def scrape_amazon_specific(self, url: str, headers: Dict, proxy_dict: Dict = None) -> Dict:
        """Amazon-specific scraping strategy with proxy support"""
        print("   📦 Using Amazon-specific strategy with IP rotation...")
        
        amazon_headers = headers.copy()
        amazon_headers.update({
            'Referer': 'https://www.google.com/',
            'Accept-Language': 'en-US,en;q=0.9',
        })
        
        response = self.session.get(url, headers=amazon_headers, proxies=proxy_dict, timeout=20)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Amazon-specific selectors
        product_info = []
        
        # Try common Amazon selectors
        selectors = [
            '#productTitle', '.product-title', 
            '#feature-bullets', '.a-unordered-list',
            '#aplus', '.aplus-module',
            '.product-description', '#productDescription'
        ]
        
        for selector in selectors:
            elements = soup.select(selector)
            for element in elements:
                text = element.get_text(strip=True)
                if text:
                    product_info.append(text)
        
        combined_text = ' '.join(product_info)
        
        return self.extract_knowledge_from_text(combined_text[:8000], url, method="amazon_specific_proxy")

    def scrape_generic_enhanced(self, url: str, headers: Dict, proxy_dict: Dict = None) -> Dict:
        """Enhanced generic scraping with proxy support"""
        print("   🌐 Using enhanced generic strategy with IP rotation...")
        
        response = self.session.get(url, headers=headers, proxies=proxy_dict, timeout=20, verify=False)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Remove unwanted elements
        for element in soup(['script', 'style', 'nav', 'footer', 'header', 'aside', 'noscript']):
            element.decompose()
        
        # Smart content extraction - look for main content areas
        content_candidates = [
            soup.find('main'),
            soup.find('article'),
            soup.find('div', class_=re.compile(r'(main|content|body|article)')),
            soup.find('div', id=re.compile(r'(main|content|body|article)')),
            soup.find('body')
        ]
        
        best_content = None
        max_text_length = 0
        
        for candidate in content_candidates:
            if candidate:
                text = candidate.get_text(strip=True)
                if len(text) > max_text_length:
                    max_text_length = len(text)
                    best_content = candidate
        
        if best_content:
            text = best_content.get_text()
            lines = (line.strip() for line in text.splitlines())
            text = ' '.join(line for line in lines if line and len(line) > 3)
        else:
            text = soup.get_text()
        
        return self.extract_knowledge_from_text(text[:10000], url, method="generic_enhanced")

    def extract_knowledge_from_text(self, text: str, source_url: str, method: str = "standard") -> Dict:
        """Enhanced knowledge extraction with domain-specific patterns"""
        
        entities = []
        relationships = []
        
        # Domain-specific entity extraction
        if 'zillow' in source_url.lower():
            entities.extend(self.extract_real_estate_entities(text))
        elif 'linkedin' in source_url.lower():
            entities.extend(self.extract_professional_entities(text))
        elif 'amazon' in source_url.lower():
            entities.extend(self.extract_product_entities(text))
        
        # General entity extraction (existing patterns)
        entities.extend(self.extract_general_entities(text))
        
        # Remove duplicates and limit
        seen_names = set()
        unique_entities = []
        for entity in entities:
            if entity['name'] not in seen_names and len(entity['name']) > 2:
                unique_entities.append(entity)
                seen_names.add(entity['name'])
                if len(unique_entities) >= 25:  # Increased limit
                    break
        
        # Create relationships between entities
        relationships = self.create_enhanced_relationships(unique_entities, text)
        
        return {
            "entities": unique_entities,
            "relationships": relationships,
            "error": False,
            "extraction_method": f"enhanced_{method}",
            "text_length": len(text),
            "domain": urlparse(source_url).netloc
        }

    def extract_real_estate_entities(self, text: str) -> List[Dict]:
        """Extract real estate specific entities"""
        entities = []
        
        # Price patterns
        prices = re.findall(r'\$[\d,]+(?:\.\d{2})?', text)
        for price in prices[:5]:
            entities.append({
                "name": price,
                "description": f"Real estate price: {price}"
            })
        
        # Address patterns
        addresses = re.findall(r'\d+\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*(?:\s+(?:St|Street|Ave|Avenue|Rd|Road|Dr|Drive|Blvd|Boulevard|Ln|Lane|Way|Ct|Court))', text)
        for address in addresses[:3]:
            entities.append({
                "name": address,
                "description": f"Property address: {address}"
            })
        
        # Property features
        features = re.findall(r'\b(\d+)\s+(bed|bedroom|bath|bathroom|garage|car|sqft|sq ft|acre)s?\b', text, re.IGNORECASE)
        for count, feature in features[:5]:
            entities.append({
                "name": f"{count} {feature}",
                "description": f"Property feature: {count} {feature}"
            })
        
        return entities

    def extract_professional_entities(self, text: str) -> List[Dict]:
        """Extract LinkedIn/professional entities"""
        entities = []
        
        # Job titles
        job_titles = re.findall(r'\b(?:Senior|Junior|Lead|Principal|Director|Manager|Engineer|Developer|Analyst|Specialist|Coordinator|Executive|VP|Vice President|CEO|CTO|CFO)\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*', text)
        for title in job_titles[:5]:
            entities.append({
                "name": title,
                "description": f"Job title: {title}"
            })
        
        # Companies
        company_patterns = re.findall(r'(?:at|@|works at|employed by)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*(?:\s+Inc|LLC|Corp|Company|Ltd)?)', text)
        for company in company_patterns[:5]:
            entities.append({
                "name": company,
                "description": f"Company: {company}"
            })
        
        return entities

    def extract_product_entities(self, text: str) -> List[Dict]:
        """Extract product/e-commerce entities"""
        entities = []
        
        # Brands
        brands = re.findall(r'\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s+(?:Brand|by|from)', text)
        for brand in brands[:5]:
            brand_clean = brand.replace(' Brand', '').replace(' by', '').replace(' from', '').strip()
            entities.append({
                "name": brand_clean,
                "description": f"Product brand: {brand_clean}"
            })
        
        # Product features
        features = re.findall(r'\b(wireless|bluetooth|waterproof|rechargeable|portable|digital|smart|premium|professional|heavy duty)\b', text, re.IGNORECASE)
        for feature in set(features[:10]):  # Remove duplicates
            entities.append({
                "name": feature.title(),
                "description": f"Product feature: {feature}"
            })
        
        return entities

    def extract_general_entities(self, text: str) -> List[Dict]:
        """General entity extraction (existing logic)"""
        entities = []
        
        # Proper nouns
        proper_nouns = re.findall(r'\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b', text)
        
        # Technical terms
        tech_terms = re.findall(r'\b(?:API|SDK|framework|library|database|server|client|protocol|algorithm|software|platform|system|application|service|tool|technology)\b', text, re.IGNORECASE)
        
        # Organizations
        org_terms = re.findall(r'\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s+(?:Inc|LLC|Corp|Company|Ltd|Organization|Foundation|Institute)\b', text)
        
        all_terms = proper_nouns + tech_terms + org_terms
        
        seen = set()
        for term in all_terms:
            term = term.strip()
            if (len(term) > 2 and len(term) < 50 and 
                term not in seen and 
                not term.isupper() and
                not re.match(r'^\d+$', term)):
                
                entities.append({
                    "name": term,
                    "description": f"Entity extracted from content: {term}"
                })
                seen.add(term)
                
                if len(entities) >= 15:
                    break
        
        return entities

    def create_enhanced_relationships(self, entities: List[Dict], text: str) -> List[Dict]:
        """Create relationships with better context understanding"""
        relationships = []
        
        # Split text into sentences for better context
        sentences = re.split(r'[.!?]+', text)
        
        for sentence in sentences[:100]:  # Process more sentences
            sentence = sentence.strip()
            if len(sentence) < 20:  # Skip very short sentences
                continue
                
            sentence_entities = [e for e in entities if e['name'].lower() in sentence.lower()]
            
            # Create relationships between entities in the same sentence
            for i, entity1 in enumerate(sentence_entities):
                for entity2 in sentence_entities[i+1:]:
                    if len(relationships) >= 20:  # Increased limit
                        break
                        
                    rel_type = self.determine_enhanced_relationship_type(sentence, entity1['name'], entity2['name'])
                    
                    relationships.append({
                        "entity1": {"name": entity1['name']},
                        "entity2": {"name": entity2['name']},
                        "relation_type": rel_type,
                        "description": f"Relationship found: {sentence[:150]}..."
                    })
        
        return relationships

    def determine_enhanced_relationship_type(self, sentence: str, entity1: str, entity2: str) -> str:
        """Enhanced relationship type detection"""
        sentence_lower = sentence.lower()
        
        # Location relationships
        if any(word in sentence_lower for word in ['located', 'in', 'at', 'near', 'address']):
            return "LOCATED_AT"
        
        # Ownership/belonging
        if any(word in sentence_lower for word in ['owns', 'belongs to', 'property of', 'owned by']):
            return "OWNS"
        
        # Professional relationships
        if any(word in sentence_lower for word in ['works at', 'employed by', 'ceo of', 'manager of']):
            return "EMPLOYED_BY"
        
        # Product relationships
        if any(word in sentence_lower for word in ['manufactured by', 'made by', 'brand']):
            return "MANUFACTURED_BY"
        
        # Price relationships
        if any(word in sentence_lower for word in ['costs', 'priced at', '$', 'price']):
            return "PRICED_AT"
        
        # Original relationship types
        if any(word in sentence_lower for word in ['uses', 'implements', 'utilizes']):
            return "USES"
        elif any(word in sentence_lower for word in ['creates', 'generates', 'produces']):
            return "CREATES"
        elif any(word in sentence_lower for word in ['part of', 'component of', 'belongs to']):
            return "PART_OF"
        elif any(word in sentence_lower for word in ['similar to', 'like', 'comparable']):
            return "SIMILAR_TO"
        elif any(word in sentence_lower for word in ['works with', 'integrates', 'connects']):
            return "INTEGRATES_WITH"
        else:
            return "RELATED_TO"

    def find_links(self, url: str, patterns: List[str] = None) -> List[str]:
        """Enhanced link finding with better filtering"""
        try:
            headers = self.get_random_headers()
            response = self.session.get(url, headers=headers, timeout=10)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            links = []
            base_domain = urlparse(url).netloc
            
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                
                # Convert relative URLs to absolute
                if href.startswith('/'):
                    href = urljoin(url, href)
                elif not href.startswith('http'):
                    continue
                
                # Skip unwanted links
                if any(skip in href for skip in ['javascript:', 'mailto:', '#', 'tel:']):
                    continue
                
                # Filter by patterns or domain
                if patterns:
                    if any(re.search(pattern, href, re.IGNORECASE) for pattern in patterns):
                        links.append(href)
                else:
                    # Include same domain + major sites
                    link_domain = urlparse(href).netloc
                    if (link_domain == base_domain or 
                        any(domain in link_domain for domain in ['github.com', 'docs.', 'support.', 'help.'])):
                        links.append(href)
            
            # Remove duplicates and limit
            unique_links = list(set(links))
            return unique_links[:15]  # Increased limit
            
        except Exception as e:
            print(f"Error finding links: {e}")
            return []

def main():
    """Enhanced main function"""
    scraper = EnhancedWebScraper()
    
    # Get configuration
    start_url = os.getenv('START_URL', 'https://www.zillow.com/')
    extract_type = os.getenv('EXTRACT_TYPE', 'entities_relationships')
    max_pages = int(os.getenv('MAX_PAGES', '3'))
    
    print(f"🚀 Starting ENHANCED scraping pipeline")
    print(f"📍 Start URL: {start_url}")
    print(f"🎯 Extract type: {extract_type}")
    print(f"📊 Max pages: {max_pages}")
    print(f"🛡️  Anti-detection: ENABLED")
    
    all_knowledge = []
    urls_to_scrape = [start_url]
    
    # Find additional URLs if needed
    if extract_type in ['entities_relationships', 'links_only']:
        print("🔗 Finding additional links...")
        additional_links = scraper.find_links(start_url, ['about', 'info', 'details', 'property'])
        urls_to_scrape.extend(additional_links[:max_pages-1])
    
    # Scrape each URL with enhanced methods
    for i, url in enumerate(urls_to_scrape[:max_pages]):
        print(f"\n📖 Processing {i+1}/{min(len(urls_to_scrape), max_pages)}: {url}")
        
        knowledge = scraper.scrape_url(url)
        all_knowledge.append(knowledge)
        
        # Respectful delays with randomization
        if i < len(urls_to_scrape) - 1:
            delay = random.uniform(3, 7)  # Increased delay
            print(f"   ⏱️  Waiting {delay:.1f}s before next request...")
            time.sleep(delay)
    
    # Save results
    os.makedirs("../output", exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"../output/kb_result_{timestamp}.json"
    
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_knowledge, f, indent=2, ensure_ascii=False)
    
    # Enhanced summary
    total_entities = sum(len(k.get("entities", [])) for k in all_knowledge if not k.get("error"))
    total_relationships = sum(len(k.get("relationships", [])) for k in all_knowledge if not k.get("error"))
    successful_scrapes = len([k for k in all_knowledge if not k.get("error")])
    failed_scrapes = len([k for k in all_knowledge if k.get("error")])
    
    summary = {
        "pipeline_status": "completed",
        "timestamp": datetime.now().isoformat(),
        "start_url": start_url,
        "extract_type": extract_type,
        "urls_processed": len(urls_to_scrape),
        "successful_scrapes": successful_scrapes,
        "failed_scrapes": failed_scrapes,
        "total_entities": total_entities,
        "total_relationships": total_relationships,
        "output_file": output_file,
        "scraping_method": "enhanced_anti_detection",
        "data": all_knowledge
    }
    
    with open("../output/scraping_summary.json", "w") as f:
        json.dump(summary, f, indent=2)
    
    print(f"\n✅ Enhanced scraping completed!")
    print(f"📊 Stats: {total_entities} entities, {total_relationships} relationships")
    print(f"✅ Success: {successful_scrapes}/{len(urls_to_scrape)} URLs")
    if failed_scrapes > 0:
        print(f"❌ Failed: {failed_scrapes} URLs")
    print(f"💾 Results saved to: {output_file}")
    
    return summary

if __name__ == "__main__":
    main()
