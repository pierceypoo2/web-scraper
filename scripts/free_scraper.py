import os
import json
import requests
from typing import Dict, List
from datetime import datetime

class SimpleZillowScraper:
    def __init__(self):
        self.rapidapi_key = os.getenv('RAPIDAPI_KEY')
        
        if not self.rapidapi_key:
            print("❌ ERROR: RAPIDAPI_KEY environment variable not found!")
            print("   Please add your RapidAPI key to GitHub Secrets")
            exit(1)
        
        print("✅ RapidAPI key found")

    def search_properties(self, location: str) -> Dict:
        """Search for properties in a location using RapidAPI"""
        print(f"🔍 Searching Zillow for: {location}")
        
        url = "https://zillow-com1.p.rapidapi.com/propertyExtendedSearch"
        
        headers = {
            "X-RapidAPI-Key": self.rapidapi_key,
            "X-RapidAPI-Host": "zillow-com1.p.rapidapi.com"
        }
        
        params = {
            "location": location,
            "status_type": "ForSale",
            "home_type": "Houses"
        }
        
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            print(f"📡 API Response Status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                return {"success": True, "data": data}
            elif response.status_code == 403:
                return {"success": False, "error": "API key invalid or quota exceeded"}
            elif response.status_code == 429:
                return {"success": False, "error": "Rate limit exceeded - try again later"}
            else:
                return {"success": False, "error": f"API returned status {response.status_code}"}
                
        except Exception as e:
            print(f"❌ API request failed: {e}")
            return {"success": False, "error": str(e)}

    def convert_to_knowledge_graph(self, api_data: Dict) -> Dict:
        """Convert Zillow API response to knowledge graph format"""
        entities = []
        relationships = []
        
        if not api_data.get("success"):
            return {
                "entities": [],
                "relationships": [],
                "error": True,
                "message": api_data.get("error", "Unknown API error")
            }
        
        try:
            properties = api_data["data"].get("props", [])
            print(f"📊 Processing {len(properties)} properties")
            
            for i, prop in enumerate(properties[:10]):  # Limit to 10 properties
                # Property address
                address_data = prop.get("address", {})
                street = address_data.get("streetAddress", "")
                city = address_data.get("city", "")
                state = address_data.get("state", "")
                zipcode = address_data.get("zipcode", "")
                
                full_address = f"{street} {city} {state} {zipcode}".strip()
                
                if full_address:
                    # Add property as entity
                    entities.append({
                        "name": full_address,
                        "description": f"Property listing from Zillow"
                    })
                    
                    # Add price as entity
                    price = prop.get("price")
                    if price:
                        price_str = f"${price:,}"
                        entities.append({
                            "name": price_str,
                            "description": f"Listing price for {full_address}"
                        })
                        
                        # Create relationship: Property -> PRICED_AT -> Price
                        relationships.append({
                            "entity1": {"name": full_address},
                            "entity2": {"name": price_str},
                            "relation_type": "PRICED_AT",
                            "description": f"{full_address} is priced at {price_str}"
                        })
                    
                    # Add bedrooms
                    bedrooms = prop.get("bedrooms")
                    if bedrooms:
                        bed_str = f"{bedrooms} bedrooms"
                        entities.append({
                            "name": bed_str,
                            "description": f"Number of bedrooms"
                        })
                        
                        relationships.append({
                            "entity1": {"name": full_address},
                            "entity2": {"name": bed_str},
                            "relation_type": "HAS_BEDROOMS",
                            "description": f"{full_address} has {bedrooms} bedrooms"
                        })
                    
                    # Add bathrooms
                    bathrooms = prop.get("bathrooms")
                    if bathrooms:
                        bath_str = f"{bathrooms} bathrooms"
                        entities.append({
                            "name": bath_str,
                            "description": f"Number of bathrooms"
                        })
                        
                        relationships.append({
                            "entity1": {"name": full_address},
                            "entity2": {"name": bath_str},
                            "relation_type": "HAS_BATHROOMS",
                            "description": f"{full_address} has {bathrooms} bathrooms"
                        })
                    
                    # Add square footage
                    sqft = prop.get("livingArea")
                    if sqft:
                        sqft_str = f"{sqft:,} sqft"
                        entities.append({
                            "name": sqft_str,
                            "description": f"Living area square footage"
                        })
                        
                        relationships.append({
                            "entity1": {"name": full_address},
                            "entity2": {"name": sqft_str},
                            "relation_type": "HAS_AREA",
                            "description": f"{full_address} has {sqft:,} square feet"
                        })
            
            print(f"✅ Created {len(entities)} entities and {len(relationships)} relationships")
            
        except Exception as e:
            print(f"❌ Error processing API data: {e}")
            return {
                "entities": [],
                "relationships": [],
                "error": True,
                "message": f"Error processing data: {str(e)}"
            }
        
        return {
            "entities": entities,
            "relationships": relationships,
            "error": False,
            "extraction_method": "zillow_api_only",
            "properties_processed": len(properties)
        }

def main():
    """Simple main function - just Zillow API"""
    scraper = SimpleZillowScraper()
    
    # Get search location from environment
    location = os.getenv('START_URL', 'Los Angeles, CA')
    
    # If it's a full URL, extract location
    if 'zillow.com' in location:
        # Try to extract city from URL
        if '/homes/for_sale/' in location:
            # Extract city from URL like /homes/for_sale/Los-Angeles-CA/
            parts = location.split('/homes/for_sale/')
            if len(parts) > 1:
                location_part = parts[1].split('/')[0]
                location = location_part.replace('-', ' ')
        else:
            # Default fallback
            location = 'Los Angeles, CA'
    
    print(f"🚀 Simple Zillow API Scraper")
    print(f"📍 Searching: {location}")
    
    # Search properties
    api_result = scraper.search_properties(location)
    
    # Convert to knowledge graph
    knowledge = scraper.convert_to_knowledge_graph(api_result)
    
    # Save results
    os.makedirs("../output", exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"../output/kb_result_{timestamp}.json"
    
    # Save as array (expected format)
    output_data = [knowledge]
    
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    # Create summary
    total_entities = len(knowledge.get("entities", []))
    total_relationships = len(knowledge.get("relationships", []))
    
    summary = {
        "pipeline_status": "completed",
        "timestamp": datetime.now().isoformat(),
        "search_location": location,
        "method": "zillow_api_only",
        "successful": not knowledge.get("error", False),
        "total_entities": total_entities,
        "total_relationships": total_relationships,
        "output_file": output_file,
        "properties_found": knowledge.get("properties_processed", 0),
        "data": output_data
    }
    
    with open("../output/scraping_summary.json", "w") as f:
        json.dump(summary, f, indent=2)
    
    # Print results
    if knowledge.get("error"):
        print(f"\n❌ Scraping failed: {knowledge.get('message')}")
    else:
        print(f"\n✅ Scraping completed!")
        print(f"🏠 Properties found: {knowledge.get('properties_processed', 0)}")
        print(f"📊 Entities: {total_entities}")
        print(f"🔗 Relationships: {total_relationships}")
        print(f"💾 Saved to: {output_file}")
    
    return summary

if __name__ == "__main__":
    main()
