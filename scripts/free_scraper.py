import os
import json
import requests
import re
import time
from typing import List, Dict, Optional
from datetime import datetime
from bs4 import BeautifulSoup
from pydantic import BaseModel
from urllib.parse import urljoin, urlparse

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

class ZillowAPIWebScraper:
    def __init__(self):
        self.session = requests.Session()
        
        # RapidAPI configuration for Zillow
        self.rapidapi_key = os.getenv('RAPIDAPI_KEY', None)
        
        # Standard headers for regular web scraping
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive'
        })

    def scrape_url(self, url: str) -> Dict:
        """Smart URL routing - API for Zillow, regular scraping for others"""
        print(f"🔍 Smart scraping: {url}")
        
        if self.is_zillow_url(url):
            return self.scrape_zillow_with_api(url)
        else:
            return self.scrape_regular_website(url)

    def is_zillow_url(self, url: str) -> bool:
        """Check if URL is a Zillow property URL"""
        return 'zillow.com' in url.lower()

    def scrape_zillow_with_api(self, url: str) -> Dict:
        """Use Zillow RapidAPI to get property data"""
        print("   🏠 Using Zillow RapidAPI...")
        
        if not self.rapidapi_key:
            print("   ⚠️ No RapidAPI key found, falling back to basic scraping")
            return self.scrape_regular_website(url)
        
        try:
            # Extract property details from Zillow URL
            zpid = self.extract_zpid_from_url(url)
            
            if zpid:
                # Use RapidAPI to get property details
                property_data = self.get_property_details_from_api(zpid)
                if property_data:
                    return self.convert_zillow_api_to_knowledge(property_data, url)
            
            # If API fails, try to search by address
            address = self.extract_address_from_url(url)
            if address:
                search_results = self.search_zillow_by_address(address)
                if search_results:
                    return self.convert_zillow_search_to_knowledge(search_results, url)
        
        except Exception as e:
            print(f"   ❌ Zillow API failed: {e}")
        
        # Fallback to regular scraping
        print("   🔄 Falling back to regular scraping...")
        return self.scrape_regular_website(url)

    def extract_zpid_from_url(self, url: str) -> Optional[str]:
        """Extract Zillow Property ID (ZPID) from URL"""
        # Zillow URLs often contain zpid in the path or as parameter
        zpid_match = re.search(r'zpid[=/_](\d+)', url, re.IGNORECASE)
        if zpid_match:
            return zpid_match.group(1)
        
        # Sometimes it's at the end of the URL path
        path_match = re.search(r'/(\d{8,})', url)
        if path_match:
            return path_match.group(1)
        
        return None

    def extract_address_from_url(self, url: str) -> Optional[str]:
        """Extract address from Zillow URL for search"""
        # Zillow URLs often have address in path like /123-main-st-city-state-zipcode/
        path = urlparse(url).path
        address_match = re.search(r'/([^/]+(?:-[A-Z]{2}-\d{5})?)', path)
        if address_match:
            address = address_match.group(1).replace('-', ' ')
            return address
        return None

    def get_property_details_from_api(self, zpid: str) -> Optional[Dict]:
        """Get property details using RapidAPI"""
        api_url = "https://zillow-com1.p.rapidapi.com/property"
        
        headers = {
            "X-RapidAPI-Key": self.rapidapi_key,
            "X-RapidAPI-Host": "zillow-com1.p.rapidapi.com"
        }
        
        params = {"zpid": zpid}
        
        try:
            print(f"   🔑 Making API call for ZPID: {zpid}")
            response = requests.get(api_url, headers=headers, params=params, timeout=15)
            if response.status_code == 200:
                print("   ✅ API call successful")
                return response.json()
            else:
                print(f"   ❌ API returned status: {response.status_code}")
        except Exception as e:
            print(f"   ❌ API request failed: {e}")
        
        return None

    def search_zillow_by_address(self, address: str) -> Optional[Dict]:
        """Search Zillow by address using RapidAPI"""
        api_url = "https://zillow-com1.p.rapidapi.com/propertyExtendedSearch"
        
        headers = {
            "X-RapidAPI-Key": self.rapidapi_key,
            "X-RapidAPI-Host": "zillow-com1.p.rapidapi.com"
        }
        
        params = {
            "location": address,
            "status_type": "ForSale"
        }
        
        try:
            print(f"   🔍 Searching for address: {address}")
            response = requests.get(api_url, headers=headers, params=params, timeout=15)
            if response.status_code == 200:
                print("   ✅ Search API successful")
                return response.json()
            else:
                print(f"   ❌ Search API returned status: {response.status_code}")
        except Exception as e:
            print(f"   ❌ Search API failed: {e}")
        
        return None

    def convert_zillow_api_to_knowledge(self, property_data: Dict, source_url: str) -> Dict:
        """Convert Zillow API response to knowledge graph format"""
        entities = []
        relationships = []
        
        try:
            print("   🔄 Converting API data to knowledge graph...")
            
            # Extract property information
            if 'address' in property_data:
                address_info = property_data['address']
                full_address = f"{address_info.get('streetAddress', '')} {address_info.get('city', '')} {address_info.get('state', '')} {address_info.get('zipcode', '')}"
                entities.append({
                    "name": full_address.strip(),
                    "description": f"Property address from Zillow API"
                })
            
            # Price information
            if 'price' in property_data:
                price = property_data['price']
                entities.append({
                    "name": f"${price:,}",
                    "description": f"Property listing price"
                })
            
            # Property details
            details = property_data.get('resoFacts', {})
            
            if 'bedrooms' in details:
                entities.append({
                    "name": f"{details['bedrooms']} bedrooms",
                    "description": "Number of bedrooms"
                })
            
            if 'bathrooms' in details:
                entities.append({
                    "name": f"{details['bathrooms']} bathrooms", 
                    "description": "Number of bathrooms"
                })
            
            if 'livingArea' in details:
                entities.append({
                    "name": f"{details['livingArea']} sqft",
                    "description": "Living area square footage"
                })
            
            if 'lotSize' in details:
                entities.append({
                    "name": f"{details['lotSize']} sqft lot",
                    "description": "Lot size"
                })
            
            # Property type
            if 'homeType' in property_data:
                entities.append({
                    "name": property_data['homeType'],
                    "description": f"Property type: {property_data['homeType']}"
                })
            
            # Zestimate
            if 'zestimate' in property_data:
                entities.append({
                    "name": f"Zestimate: ${property_data['zestimate']:,}",
                    "description": "Zillow's automated property valuation"
                })
            
            # Create relationships between property and its features
            if entities:
                property_entity = entities[0]
                
                for i, entity in enumerate(entities[1:], 1):
                    relationships.append({
                        "entity1": {"name": property_entity["name"]},
                        "entity2": {"name": entity["name"]},
                        "relation_type": "HAS_FEATURE",
                        "description": f"Property has feature: {entity['name']}"
                    })
            
            print(f"   ✅ Extracted {len(entities)} entities and {len(relationships)} relationships")
        
        except Exception as e:
            print(f"   ❌ Error processing API data: {e}")
            return {"error": True, "message": str(e), "source_url": source_url}
        
        return {
            "entities": entities,
            "relationships": relationships,
            "error": False,
            "extraction_method": "zillow_rapidapi",
            "api_data_keys": list(property_data.keys()) if property_data else [],
            "source_url": source_url
        }

    def convert_zillow_search_to_knowledge(self, search_results: Dict, source_url: str) -> Dict:
        """Convert Zillow search results to knowledge graph"""
        entities = []
        relationships = []
        
        try:
            print("   🔄 Converting search results to knowledge graph...")
            
            props = search_results.get('props', [])
            
            for prop in props[:5]:  # Limit to first 5 properties
                # Property address
                address = prop.get('address', {})
                full_address = f"{address.get('streetAddress', '')} {address.get('city', '')} {address.get('state', '')}"
                
                if full_address.strip():
                    entities.append({
                        "name": full_address.strip(),
                        "description": "Property from search results"
                    })
                    
                    # Price
                    if 'price' in prop and prop['price']:
                        price_str = f"${prop['price']:,}"
                        entities.append({
                            "name": price_str,
                            "description": f"Price for {full_address.strip()}"
                        })
                        
                        # Create price relationship
                        relationships.append({
                            "entity1": {"name": full_address.strip()},
                            "entity2": {"name": price_str},
                            "relation_type": "PRICED_AT", 
                            "description": f"Property priced at {price_str}"
                        })
                    
                    # Bedrooms/Bathrooms
                    if 'bedrooms' in prop and prop['bedrooms']:
                        bed_entity = f"{prop['bedrooms']} bed"
                        entities.append({
                            "name": bed_entity,
                            "description": "Number of bedrooms"
                        })
                        
                        relationships.append({
                            "entity1": {"name": full_address.strip()},
                            "entity2": {"name": bed_entity},
                            "relation_type": "HAS_BEDROOMS",
                            "description": f"Property has {prop['bedrooms']} bedrooms"
                        })
            
            print(f"   ✅ Extracted {len(entities)} entities from search results")
        
        except Exception as e:
            print(f"   ❌ Error processing search results: {e}")
        
        return {
            "entities": entities,
            "relationships": relationships, 
            "error": False,
            "extraction_method": "zillow_search_api",
            "source_url": source_url
        }

    def scrape_regular_website(self, url: str) -> Dict:
        """Regular web scraping for non-Zillow URLs"""
        print("   🌐 Using regular web scraping...")
        
        try:
            response = self.session.get(url, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Remove unwanted elements
            for element in soup(['script', 'style', 'nav', 'footer', 'header', 'aside']):
                element.decompose()
            
            # Extract text content
            text = soup.get_text()
            lines = (line.strip() for line in text.splitlines())
            text = ' '.join(line for line in lines if line and len(line) > 3)
            
            return self.extract_knowledge_from_text(text[:8000], url)
            
        except Exception as e:
            print(f"   ❌ Regular scraping failed: {e}")
            return {"error": True, "message": str(e), "source_url": url}

    def extract_knowledge_from_text(self, text: str, source_url: str) -> Dict:
        """Standard knowledge extraction for regular websites"""
        entities = []
        relationships = []
        
        # Find proper nouns (potential entities)
        proper_nouns = re.findall(r'\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b', text)
        
        # Find technical terms
        tech_terms = re.findall(r'\b(?:API|SDK|framework|library|database|server|client|protocol|algorithm)\b', text, re.IGNORECASE)
        
        # Find quoted terms
        quoted_terms = re.findall(r'"([^"]{3,30})"', text)
        
        # Combine and deduplicate
        all_terms = list(set(proper_nouns + tech_terms + quoted_terms))
        
        for term in all_terms[:15]:  # Limit entities
            if len(term) > 2 and len(term) < 50 and not term.isupper():
                entities.append({
                    "name": term,
                    "description": f"Entity from {urlparse(source_url).netloc}: {term}"
                })
        
        # Create simple relationships
        sentences = re.split(r'[.!?]+', text)
        for sentence in sentences[:20]:
            sentence_entities = [e for e in entities if e['name'].lower() in sentence.lower()]
            
            for i, entity1 in enumerate(sentence_entities):
                for entity2 in sentence_entities[i+1:]:
                    if len(relationships) >= 10:
                        break
                    
                    relationships.append({
                        "entity1": {"name": entity1['name']},
                        "entity2": {"name": entity2['name']},
                        "relation_type": "RELATED_TO",
                        "description": f"Entities mentioned together: {sentence[:100]}..."
                    })
        
        return {
            "entities": entities,
            "relationships": relationships,
            "error": False,
            "extraction_method": "standard_scraping",
            "source_url": source_url
        }

    def find_links(self, url: str, patterns: List[str] = None) -> List[str]:
        """Find related links on a page"""
        try:
            response = self.session.get(url, timeout=10)
            soup = BeautifulSoup(response.content, 'html.parser')
            
            links = []
            base_domain = urlparse(url).netloc
            
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                
                if href.startswith('/'):
                    href = urljoin(url, href)
                elif not href.startswith('http'):
                    continue
                
                if patterns:
                    if any(re.search(pattern, href, re.IGNORECASE) for pattern in patterns):
                        links.append(href)
                else:
                    link_domain = urlparse(href).netloc
                    if link_domain == base_domain:
                        links.append(href)
            
            return list(set(links))[:10]
            
        except Exception as e:
            print(f"Error finding links: {e}")
            return []

def main():
    """Main execution with Zillow API support"""
    scraper = ZillowAPIWebScraper()
    
    # Get configuration from environment
    start_url = os.getenv('START_URL', 'https://www.zillow.com/')
    extract_type = os.getenv('EXTRACT_TYPE', 'entities_relationships')
    max_pages = int(os.getenv('MAX_PAGES', '3'))
    rapidapi_key = os.getenv('RAPIDAPI_KEY', None)
    
    print(f"🚀 Starting Zillow API + Web Scraping pipeline")
    print(f"📍 Start URL: {start_url}")
    print(f"🎯 Extract type: {extract_type}")
    print(f"📊 Max pages: {max_pages}")
    
    if rapidapi_key:
        print("🔑 RapidAPI key detected - Zillow API enabled")
    else:
        print("⚠️ No RapidAPI key - using fallback scraping for Zillow")
    
    all_knowledge = []
    urls_to_scrape = [start_url]
    
    # Find additional URLs if needed
    if extract_type in ['entities_relationships', 'links_only'] and not scraper.is_zillow_url(start_url):
        print("🔗 Finding additional links...")
        additional_links = scraper.find_links(start_url, ['about', 'property', 'listing'])
        urls_to_scrape.extend(additional_links[:max_pages-1])
    
    # Scrape each URL
    for i, url in enumerate(urls_to_scrape[:max_pages]):
        print(f"\n📖 Processing {i+1}/{min(len(urls_to_scrape), max_pages)}: {url}")
        
        knowledge = scraper.scrape_url(url)
        all_knowledge.append(knowledge)
        
        # Be respectful - add delay
        if i < len(urls_to_scrape) - 1:
            time.sleep(2)
    
    # Save results
    os.makedirs("../output", exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"../output/kb_result_{timestamp}.json"
    
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_knowledge, f, indent=2, ensure_ascii=False)
    
    # Create summary
    total_entities = sum(len(k.get("entities", [])) for k in all_knowledge if not k.get("error"))
    total_relationships = sum(len(k.get("relationships", [])) for k in all_knowledge if not k.get("error"))
    successful_scrapes = len([k for k in all_knowledge if not k.get("error")])
    api_scrapes = len([k for k in all_knowledge if k.get("extraction_method", "").startswith("zillow_")])
    
    summary = {
        "pipeline_status": "completed",
        "timestamp": datetime.now().isoformat(),
        "start_url": start_url,
        "extract_type": extract_type,
        "urls_processed": len(urls_to_scrape),
        "successful_scrapes": successful_scrapes,
        "api_enhanced_scrapes": api_scrapes,
        "total_entities": total_entities,
        "total_relationships": total_relationships,
        "output_file": output_file,
        "data": all_knowledge
    }
    
    with open("../output/scraping_summary.json", "w") as f:
        json.dump(summary, f, indent=2)
    
    print(f"\n✅ Scraping completed!")
    print(f"📊 Stats: {total_entities} entities, {total_relationships} relationships")
    print(f"🏠 Zillow API calls: {api_scrapes}")
    print(f"💾 Results saved to: {output_file}")
    
    return summary

if __name__ == "__main__":
    main()
