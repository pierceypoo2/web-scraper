import os
import json
import requests
from typing import Dict, List
from datetime import datetime

class WorkingZillowScraper:
    def __init__(self):
        self.rapidapi_key = os.getenv('RAPIDAPI_KEY')
        
        if not self.rapidapi_key:
            print("❌ ERROR: RAPIDAPI_KEY environment variable not found!")
            print("   Please add your RapidAPI key to GitHub Secrets")
            exit(1)
        
        print("✅ RapidAPI key found")

    def search_properties(self, location: str) -> Dict:
        """Search for properties using the Working Zillow API"""
        print(f"🔍 Searching Zillow for: {location}")
        
        # Use the Working Zillow API endpoint
        url = "https://zillow-working-api.p.rapidapi.com/byCity"
        
        headers = {
            "X-RapidAPI-Key": self.rapidapi_key,
            "X-RapidAPI-Host": "zillow-working-api.p.rapidapi.com"
        }
        
        params = {
            "location": location
        }
        
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            print(f"📡 API Response Status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                print(f"✅ API call successful - got {len(data.get('results', []))} results")
                return {"success": True, "data": data}
            elif response.status_code == 403:
                print("❌ 403 Forbidden - Check your API key")
                return {"success": False, "error": "API key invalid or not subscribed to this API"}
            elif response.status_code == 429:
                print("❌ 429 Rate Limited")
                return {"success": False, "error": "Rate limit exceeded - try again later"}
            else:
                print(f"❌ Unexpected status: {response.status_code}")
                print(f"Response: {response.text[:500]}")
                return {"success": False, "error": f"API returned status {response.status_code}"}
                
        except Exception as e:
            print(f"❌ API request failed: {e}")
            return {"success": False, "error": str(e)}

    def try_backup_api(self, location: str) -> Dict:
        """Try the backup Zillow API if main one fails"""
        print(f"🔄 Trying backup API for: {location}")
        
        url = "https://zillow-com1.p.rapidapi.com/propertyExtendedSearch"
        
        headers = {
            "X-RapidAPI-Key": self.rapidapi_key,
            "X-RapidAPI-Host": "zillow-com1.p.rapidapi.com"
        }
        
        params = {
            "location": location,
            "status_type": "ForSale"
        }
        
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            print(f"📡 Backup API Response Status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                return {"success": True, "data": data}
            else:
                return {"success": False, "error": f"Backup API returned status {response.status_code}"}
                
        except Exception as e:
            print(f"❌ Backup API failed: {e}")
            return {"success": False, "error": str(e)}

    def convert_to_knowledge_graph(self, api_data: Dict) -> Dict:
        """Convert API response to knowledge graph format"""
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
            # Handle different API response formats
            data = api_data["data"]
            
            # Try to find properties in different possible locations
            properties = []
            if "results" in data:
                properties = data["results"]
            elif "props" in data:
                properties = data["props"]
            elif isinstance(data, list):
                properties = data
            else:
                # If data is a single property
                properties = [data]
            
            print(f"📊 Processing {len(properties)} properties")
            
            for i, prop in enumerate(properties[:10]):  # Limit to 10 properties
                if not isinstance(prop, dict):
                    continue
                    
                # Try to extract address (different APIs format differently)
                address = ""
                if "address" in prop:
                    addr_data = prop["address"]
                    if isinstance(addr_data, dict):
                        street = addr_data.get("streetAddress", "")
                        city = addr_data.get("city", "")
                        state = addr_data.get("state", "")
                        zipcode = addr_data.get("zipcode", "")
                        address = f"{street} {city} {state} {zipcode}".strip()
                    else:
                        address = str(addr_data)
                elif "fullAddress" in prop:
                    address = prop["fullAddress"]
                elif "street" in prop and "city" in prop:
                    address = f"{prop.get('street', '')} {prop.get('city', '')} {prop.get('state', '')}".strip()
                
                if not address:
                    address = f"Property {i+1}"
                
                # Add property as entity
                entities.append({
                    "name": address,
                    "description": f"Zillow property listing"
                })
                
                # Extract and add price
                price = None
                for price_field in ["price", "listPrice", "amount", "rentAmount"]:
                    if price_field in prop and prop[price_field]:
                        price = prop[price_field]
                        break
                
                if price:
                    try:
                        price_num = int(price) if isinstance(price, (int, float)) else int(str(price).replace('$', '').replace(',', ''))
                        price_str = f"${price_num:,}"
                        
                        entities.append({
                            "name": price_str,
                            "description": f"Listing price"
                        })
                        
                        relationships.append({
                            "entity1": {"name": address},
                            "entity2": {"name": price_str},
                            "relation_type": "PRICED_AT",
                            "description": f"Property is priced at {price_str}"
                        })
                    except:
                        pass
                
                # Extract bedrooms
                bedrooms = prop.get("bedrooms") or prop.get("beds")
                if bedrooms:
                    bed_str = f"{bedrooms} bedrooms"
                    entities.append({
                        "name": bed_str,
                        "description": "Number of bedrooms"
                    })
                    
                    relationships.append({
                        "entity1": {"name": address},
                        "entity2": {"name": bed_str},
                        "relation_type": "HAS_BEDROOMS",
                        "description": f"Property has {bedrooms} bedrooms"
                    })
                
                # Extract bathrooms
                bathrooms = prop.get("bathrooms") or prop.get("baths")
                if bathrooms:
                    bath_str = f"{bathrooms} bathrooms"
                    entities.append({
                        "name": bath_str,
                        "description": "Number of bathrooms"
                    })
                    
                    relationships.append({
                        "entity1": {"name": address},
                        "entity2": {"name": bath_str},
                        "relation_type": "HAS_BATHROOMS",
                        "description": f"Property has {bathrooms} bathrooms"
                    })
                
                # Extract square footage
                sqft = prop.get("livingArea") or prop.get("sqft") or prop.get("area")
                if sqft:
                    try:
                        sqft_num = int(sqft)
                        sqft_str = f"{sqft_num:,} sqft"
                        
                        entities.append({
                            "name": sqft_str,
                            "description": "Living area"
                        })
                        
                        relationships.append({
                            "entity1": {"name": address},
                            "entity2": {"name": sqft_str},
                            "relation_type": "HAS_AREA",
                            "description": f"Property has {sqft_num:,} square feet"
                        })
                    except:
                        pass
            
            print(f"✅ Created {len(entities)} entities and {len(relationships)} relationships")
            
        except Exception as e:
            print(f"❌ Error processing API data: {e}")
            print(f"Raw data structure: {list(api_data.get('data', {}).keys()) if isinstance(api_data.get('data'), dict) else type(api_data.get('data'))}")
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
            "extraction_method": "zillow_working_api",
            "properties_processed": len(properties)
        }

def main():
    """Main function with fallback APIs"""
    scraper = WorkingZillowScraper()
    
    # Get search location from environment
    location = os.getenv('START_URL', 'Los Angeles, CA')
    
    # Clean up location if it's a Zillow URL
    if 'zillow.com' in location:
        if '/homes/for_sale/' in location:
            parts = location.split('/homes/for_sale/')
            if len(parts) > 1:
                location_part = parts[1].split('/')[0]
                location = location_part.replace('-', ' ').replace('_', ' ')
        else:
            location = 'Los Angeles, CA'
    
    print(f"🚀 Working Zillow API Scraper")
    print(f"📍 Searching: {location}")
    
    # Try main API first
    api_result = scraper.search_properties(location)
    
    # If main API fails, try backup
    if not api_result.get("success"):
        print("🔄 Main API failed, trying backup...")
        api_result = scraper.try_backup_api(location)
    
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
        "method": "zillow_working_api",
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
        print("\n🔍 Troubleshooting:")
        print("1. Check that you subscribed to the API on RapidAPI")
        print("2. Verify your API key is correct") 
        print("3. Check if you have remaining quota")
    else:
        print(f"\n✅ Scraping completed!")
        print(f"🏠 Properties found: {knowledge.get('properties_processed', 0)}")
        print(f"📊 Entities: {total_entities}")
        print(f"🔗 Relationships: {total_relationships}")
        print(f"💾 Saved to: {output_file}")
    
    return summary

if __name__ == "__main__":
    main()
