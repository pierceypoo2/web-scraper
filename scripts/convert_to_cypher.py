import os
import json
import glob
from datetime import datetime
from typing import Dict, List

class CypherGenerator:
    def __init__(self):
        self.entity_counter = 0
        self.relationship_counter = 0
        
    def escape_string(self, text: str) -> str:
        """Escape special characters for Cypher queries"""
        if not text:
            return ""
        return text.replace('"', '\\"').replace("'", "\\'").replace('\n', ' ').replace('\r', ' ')
    
    def convert_entities_to_cypher(self, entities: List[Dict]) -> str:
        """Convert entities to Cypher CREATE statements"""
        queries = []
        
        for entity in entities:
            name = self.escape_string(entity.get('name', ''))
            description = self.escape_string(entity.get('description', ''))
            
            if name:  # Only create if name exists
                query = f'CREATE (e{self.entity_counter}:Entity {{name: "{name}", description: "{description}"}})'
                queries.append(query)
                self.entity_counter += 1
        
        return '\n'.join(queries)

    def convert_relationships_to_cypher(self, relationships: List[Dict]) -> str:
        """Convert relationships to Cypher statements"""
        queries = []
        
        for rel in relationships:
            try:
                entity1_name = self.escape_string(rel['entity1']['name'])
                entity2_name = self.escape_string(rel['entity2']['name'])
                relation_type = self.escape_string(rel.get('relation_type', 'RELATED_TO'))
                description = self.escape_string(rel.get('description', ''))
                
                if entity1_name and entity2_name:
                    query = f"""MATCH (e1:Entity {{name: "{entity1_name}"}})
MATCH (e2:Entity {{name: "{entity2_name}"}})
CREATE (e1)-[r{self.relationship_counter}:RELATES_TO {{
    type: "{relation_type}",
    description: "{description}"
}}]->(e2)"""
                    queries.append(query)
                    self.relationship_counter += 1
                    
            except KeyError as e:
                print(f"⚠️ Skipping malformed relationship: {e}")
                continue
        
        return '\n'.join(queries)

    def create_indexes(self) -> str:
        """Create indexes for better performance"""
        return """// Create indexes for better performance
CREATE INDEX entity_name_index IF NOT EXISTS FOR (e:Entity) ON (e.name);
CREATE INDEX relationship_type_index IF NOT EXISTS FOR ()-[r:RELATES_TO]-() ON (r.type);"""

    def create_constraints(self) -> str:
        """Create uniqueness constraints"""
        return """// Create constraints
CREATE CONSTRAINT entity_name_unique IF NOT EXISTS FOR (e:Entity) REQUIRE e.name IS UNIQUE;"""

    def process_json_files(self, input_pattern: str = "../output/kb_result_*.json") -> str:
        """Process all JSON files and generate Cypher"""
        print("🔄 Processing JSON files for Cypher conversion...")
        
        json_files = glob.glob(input_pattern)
        if not json_files:
            print("❌ No JSON files found to process")
            return None
            
        print(f"📁 Found {len(json_files)} files to process")
        
        all_queries = []
        
        # Add constraints and indexes first
        all_queries.append(self.create_constraints())
        all_queries.append(self.create_indexes())
        all_queries.append("// Clear existing data (optional)")
        all_queries.append("MATCH (n) DETACH DELETE n;")
        all_queries.append("")
        all_queries.append("// Create entities and relationships")
        
        total_entities = 0
        total_relationships = 0
        
        for json_file in json_files:
            print(f"📖 Processing: {json_file}")
            
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Handle both single objects and arrays
                if not isinstance(data, list):
                    data = [data]
                
                for item in data:
                    if item.get("error", False):
                        continue
                    
                    # Process entities
                    if "entities" in item and item["entities"]:
                        entity_query = self.convert_entities_to_cypher(item["entities"])
                        if entity_query:
                            all_queries.append(f"// Entities from {item.get('source_url', 'unknown')}")
                            all_queries.append(entity_query)
                            total_entities += len(item["entities"])
                    
                    # Process relationships
                    if "relationships" in item and item["relationships"]:
                        rel_query = self.convert_relationships_to_cypher(item["relationships"])
                        if rel_query:
                            all_queries.append(f"// Relationships from {item.get('source_url', 'unknown')}")
                            all_queries.append(rel_query)
                            total_relationships += len(item["relationships"])
                            
            except Exception as e:
                print(f"❌ Error processing {json_file}: {e}")
                continue
        
        # Join all queries
        final_query = '\n'.join(all_queries)
        
        # Save to file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = "../output"
        os.makedirs(output_dir, exist_ok=True)
        
        cypher_filename = os.path.join(output_dir, f"neo4j_query_{timestamp}.cypher")
        
        with open(cypher_filename, 'w', encoding='utf-8') as f:
            f.write(final_query)
        
        # Create a summary file
        summary = {
            "cypher_file": cypher_filename,
            "total_entities": total_entities,
            "total_relationships": total_relationships,
            "files_processed": len(json_files),
            "generation_timestamp": datetime.now().isoformat(),
            "query_length": len(final_query)
        }
        
        summary_file = os.path.join(output_dir, f"cypher_summary_{timestamp}.json")
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        print(f"✅ Cypher generation completed!")
        print(f"📊 Generated {total_entities} entity statements")
        print(f"🔗 Generated {total_relationships} relationship statements") 
        print(f"💾 Saved to: {cypher_filename}")
        
        return cypher_filename

def main():
    """Main function for Cypher generation"""
    generator = CypherGenerator()
    
    # Look for JSON files to process
    cypher_file = generator.process_json_files()
    
    if cypher_file:
        print(f"\n🎉 Success! Neo4j queries ready at: {cypher_file}")
        
        # Also create a simple import script
        import_script = f"""#!/bin/bash
# Simple Neo4j import script
# Usage: ./import_to_neo4j.sh

NEO4J_URI="bolt://localhost:7687"
NEO4J_USER="neo4j"
NEO4J_PASSWORD="password"

echo "Importing data to Neo4j..."
cypher-shell -a $NEO4J_URI -u $NEO4J_USER -p $NEO4J_PASSWORD < {os.path.basename(cypher_file)}
echo "Import completed!"
"""
        
        script_path = os.path.join(os.path.dirname(cypher_file), "import_to_neo4j.sh")
        with open(script_path, 'w') as f:
            f.write(import_script)
        
        # Make it executable
        os.chmod(script_path, 0o755)
        print(f"📜 Import script created: {script_path}")
    else:
        print("❌ No Cypher file generated - check for JSON input files")

if __name__ == "__main__":
    main()
