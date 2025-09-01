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

class FreeWebScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (compatible; WebScraper/1.0; +https://github.com/yourusername/web-scraper)'
        })
        
    def scrape_url(self, url: str) -> Dict:
        """Scrape a single URL and extract knowledge"""
        print(f"🔍 Scraping: {url}")
        
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
            
            # Extract knowledge using rule-based approach
            knowledge = self.extract_knowledge_from_text(text, url)
            knowledge['source_url'] = url
            
            return knowledge
            
        except Exception as e:
            print(f"❌ Error scraping {url}: {e}")
            return {"error": True, "message": str(e), "source_url": url}

    def extract_knowledge_from_text(self, text: str, source_url: str) -> Dict:
        """Extract entities and relationships using pattern matching"""
        
        # Limit text for processing
        text = text[:8000]
        
        entities = []
        relationships = []
        
        # Pattern 1: Find proper nouns (potential entities)
        proper_nouns = re.findall(r'\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b', text)
        
        # Pattern 2: Find technical terms
        tech_terms = re.findall(r'\b(?:API|SDK|framework|library|database|server|client|protocol|algorithm)\b', text, re.IGNORECASE)
        
        # Pattern 3: Find quoted terms
        quoted_terms = re.findall(r'"([^"]{3,30})"', text)
        
        # Pattern 4: Find code-related terms
        code_terms = re.findall(r'\b(?:Python|JavaScript|HTML|CSS|React|Node|Docker|GitHub|Git)\b', text)
        
        # Combine and deduplicate entities
        all_potential_entities = proper_nouns + tech_terms + quoted_terms + code_terms
        seen_entities = set()
        
        for term in all_potential_entities:
            term = term.strip()
            if (len(term) > 2 and len(term) < 50 and 
                term not in seen_entities and 
                not term.isupper() and
                not re.match(r'^\d+$', term)):
                
                entities.append({
                    "name": term,
                    "description": f"Entity extracted from {urlparse(source_url).netloc}: {term}"
                })
                seen_entities.add(term)
                
                if len(entities) >= 20:  # Limit entities
                    break
        
        # Create relationships based on co-occurrence
        sentences = re.split(r'[.!?]+', text)
        
        for sentence in sentences[:50]:  # Limit sentences to process
            sentence_entities = [e for e in entities if e['name'].lower() in sentence.lower()]
            
            # Create relationships between entities in the same sentence
            for i, entity1 in enumerate(sentence_entities):
                for entity2 in sentence_entities[i+1:]:
                    if len(relationships) >= 15:  # Limit relationships
                        break
                        
                    # Determine relationship type based on context
                    rel_type = self.determine_relationship_type(sentence, entity1['name'], entity2['name'])
                    
                    relationships.append({
                        "entity1": {"name": entity1['name']},
                        "entity2": {"name": entity2['name']},
                        "relation_type": rel_type,
                        "description": f"Relationship found in context: {sentence[:100]}..."
                    })
        
        return {
            "entities": entities,
            "relationships": relationships,
            "error": False,
            "extraction_method": "rule_based",
            "text_length": len(text)
        }

    def determine_relationship_type(self, sentence: str, entity1: str, entity2: str) -> str:
        """Determine relationship type based on context"""
        sentence_lower = sentence.lower()
        
        # Rule-based relationship classification
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
        """Find relevant links on a page"""
        try:
            response = self.session.get(url, timeout=10)
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
                
                # Filter links
                if patterns:
                    if any(re.search(pattern, href, re.IGNORECASE) for pattern in patterns):
                        links.append(href)
                else:
                    # Only include links from same domain or major code repositories
                    link_domain = urlparse(href).netloc
                    if (link_domain == base_domain or 
                        any(repo in link_domain for repo in ['github.com', 'gitlab.com', 'docs.'])):
                        links.append(href)
            
            return list(set(links))[:10]  # Deduplicate and limit
            
        except Exception as e:
            print(f"Error finding links: {e}")
            return []

def main():
    """Main execution function"""
    scraper = FreeWebScraper()
    
    # Get configuration from environment
    start_url = os.getenv('START_URL', 'https://crawl4ai.com/')
    extract_type = os.getenv('EXTRACT_TYPE', 'entities_relationships')
    max_pages = int(os.getenv('MAX_PAGES', '3'))
    
    print(f"🚀 Starting free scraping pipeline")
    print(f"📍 Start URL: {start_url}")
    print(f"🎯 Extract type: {extract_type}")
    print(f"📊 Max pages: {max_pages}")
    
    all_knowledge = []
    urls_to_scrape = [start_url]
    
    # If extracting links, find additional URLs
    if extract_type in ['entities_relationships', 'links_only']:
        print("🔗 Finding additional links...")
        additional_links = scraper.find_links(start_url, ['docs', 'about', 'github', 'api'])
        urls_to_scrape.extend(additional_links[:max_pages-1])
    
    # Scrape each URL
    for i, url in enumerate(urls_to_scrape[:max_pages]):
        print(f"\n📖 Processing {i+1}/{min(len(urls_to_scrape), max_pages)}: {url}")
        
        knowledge = scraper.scrape_url(url)
        all_knowledge.append(knowledge)
        
        # Be respectful - add delay between requests
        if i < len(urls_to_scrape) - 1:
            time.sleep(2)
    
    # Save results
    os.makedirs("../output", exist_ok=True)
    
    # Save raw knowledge data
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"../output/kb_result_{timestamp}.json"
    
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_knowledge, f, indent=2, ensure_ascii=False)
    
    # Create summary stats
    total_entities = sum(len(k.get("entities", [])) for k in all_knowledge if not k.get("error"))
    total_relationships = sum(len(k.get("relationships", [])) for k in all_knowledge if not k.get("error"))
    successful_scrapes = len([k for k in all_knowledge if not k.get("error")])
    
    summary = {
        "pipeline_status": "completed",
        "timestamp": datetime.now().isoformat(),
        "start_url": start_url,
        "extract_type": extract_type,
        "urls_processed": len(urls_to_scrape),
        "successful_scrapes": successful_scrapes,
        "total_entities": total_entities,
        "total_relationships": total_relationships,
        "output_file": output_file,
        "data": all_knowledge
    }
    
    # Save summary
    with open("../output/scraping_summary.json", "w") as f:
        json.dump(summary, f, indent=2)
    
    print(f"\n✅ Scraping completed!")
    print(f"📊 Stats: {total_entities} entities, {total_relationships} relationships")
    print(f"💾 Results saved to: {output_file}")
    
    return summary

if __name__ == "__main__":
    main()
