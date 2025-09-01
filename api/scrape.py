# api/scrape.py - Vercel serverless function
import json
import requests
from bs4 import BeautifulSoup
import re
from typing import Dict, List

def extract_simple_knowledge(text: str) -> Dict:
    """Simple, fast entity extraction without heavy LLMs"""
    
    # Extract entities using patterns
    entities = []
    relationships = []
    
    # Find capitalized phrases (likely entities)
    entity_pattern = r'\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b'
    potential_entities = re.findall(entity_pattern, text)
    
    # Clean and deduplicate
    seen = set()
    for entity in potential_entities:
        if len(entity) > 2 and entity not in seen and not entity.isupper():
            entities.append({
                "name": entity,
                "description": f"Entity mentioned in content: {entity}"
            })
            seen.add(entity)
            if len(entities) >= 15:  # Limit for performance
                break
    
    # Create relationships between entities that appear close together
    for i in range(len(entities) - 1):
        relationships.append({
            "entity1": {"name": entities[i]["name"]},
            "entity2": {"name": entities[i+1]["name"]},
            "relation_type": "MENTIONED_WITH",
            "description": f"Entities {entities[i]['name']} and {entities[i+1]['name']} mentioned together"
        })
    
    return {
        "entities": entities,
        "relationships": relationships,
        "error": False
    }

def scrape_url(url: str) -> Dict:
    """Fast web scraping using requests + BeautifulSoup"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (compatible; WebScraper/1.0)'
        }
        
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Remove unwanted elements
        for element in soup(['script', 'style', 'nav', 'footer', 'header']):
            element.decompose()
        
        # Extract clean text
        text = soup.get_text()
        lines = (line.strip() for line in text.splitlines())
        text = ' '.join(line for line in lines if line)
        
        # Limit text length for performance
        text = text[:5000]
        
        return extract_simple_knowledge(text)
        
    except Exception as e:
        return {"error": True, "message": str(e)}

def handler(request):
    """Vercel serverless function handler"""
    if request.method == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'POST, GET, OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type',
            }
        }
    
    if request.method != 'POST':
        return {
            'statusCode': 405,
            'body': json.dumps({'error': 'Method not allowed'})
        }
    
    try:
        body = json.loads(request.body)
        url = body.get('url')
        
        if not url:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'URL is required'})
            }
        
        # Scrape and extract
        result = scrape_url(url)
        
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Content-Type': 'application/json',
            },
            'body': json.dumps(result)
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

# For local testing
if __name__ == "__main__":
    # Test the scraper
    test_url = "https://crawl4ai.com/"
    result = scrape_url(test_url)
    print(json.dumps(result, indent=2))
