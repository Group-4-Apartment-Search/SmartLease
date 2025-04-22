import re
import json
from typing import List, Dict, Any, Optional

# Snowflake integration
from snowflake.snowpark.context import get_active_session

class TransportationProAnalyzer:
    """
    Transportation-Pro system that analyzes property listings
    with a focus on transportation features, optimized for your specific data structure.
    """
    
    def __init__(self, model_name="claude-3-5-sonnet"):
        """Initialize the TransportationPro system with Snowflake integration."""
        self.model = model_name
        
        try:
            # Initialize Snowflake session
            self.session = get_active_session()
            print("Snowflake session initialized successfully.")
            
            # Test query to verify connection
            test_query = "SELECT COUNT(*) FROM LISTINGS.PUBLIC.PROPERTIES_WITH_EMBEDDING"
            result = self.session.sql(test_query).collect()
            count = result[0][0]
            print(f"Connected to database. Found {count} properties.")
            
        except Exception as e:
            print(f"Warning: Could not initialize Snowflake or access table: {e}")
    
    def complete_llm(self, prompt: str) -> str:
        """
        Execute LLM completion using SQL rather than direct import.
        
        Args:
            prompt: The prompt to send to the LLM
            
        Returns:
            LLM response text
        """
        try:
            # Escape single quotes in prompt for SQL
            safe_prompt = prompt.replace("'", "''")
            
            # Execute the LLM call through SQL
            result = self.session.sql(f"""
                SELECT snowflake.cortex.complete('{self.model}', '{safe_prompt}') AS response
            """).collect()
            
            # Extract response text
            response_text = result[0][0]
            return response_text
        except Exception as e:
            print(f"Error calling LLM through SQL: {e}")
            return "{}"  # Return empty JSON if LLM call fails
    
    def analyze_property_cot(self, property_details: Dict[str, Any], user_preferences: List[str]) -> Dict[str, Any]:
        """
        Analyze a property using Chain of Thought prompting approach.
        
        Args:
            property_details: Dictionary of property details
            user_preferences: List of user preference strings
            
        Returns:
            Analysis result including transportation features, pros, and cons
        """
        # Extract property details
        property_text = property_details.get('text', '')
        address = f"{property_details.get('street', '')} {property_details.get('unit', '')}, {property_details.get('city', '')}, {property_details.get('state', '')} {property_details.get('zip_code', '')}"
        
        # Extract POI (Points of Interest) data for transportation analysis
        poi_data = f"""
        Nearby Points of Interest:
        - Restaurants: {property_details.get('restaurant_count', 0)} nearby. Closest: {property_details.get('closest_restaurant_name', 'N/A')} ({property_details.get('closest_restaurant_distance', 'N/A')} miles)
        - Cafes: {property_details.get('cafe_count', 0)} nearby. Closest: {property_details.get('closest_cafe_name', 'N/A')} ({property_details.get('closest_cafe_distance', 'N/A')} miles)
        - Hospitals: {property_details.get('hospital_count', 0)} nearby. Closest: {property_details.get('closest_hospital_name', 'N/A')} ({property_details.get('closest_hospital_distance', 'N/A')} miles)
        - Pharmacies: {property_details.get('pharmacy_count', 0)} nearby. Closest: {property_details.get('closest_pharmacy_name', 'N/A')} ({property_details.get('closest_pharmacy_distance', 'N/A')} miles)
        - ATMs: {property_details.get('atm_count', 0)} nearby. Closest: {property_details.get('closest_atm_name', 'N/A')} ({property_details.get('closest_atm_distance', 'N/A')} miles)
        - Banks: {property_details.get('bank_count', 0)} nearby. Closest: {property_details.get('closest_bank_name', 'N/A')} ({property_details.get('closest_bank_distance', 'N/A')} miles)
        """
        
        # Combine all information
        combined_description = f"""
        Property: {property_details.get('style', '')} home
        Address: {address}
        Listing Price: ${property_details.get('list_price', 'N/A')}
        Size: {property_details.get('beds', '')} beds, {property_details.get('full_baths', '')} full baths
        Square Feet: {property_details.get('sqft', '')}
        
        Property Description:
        {property_text}
        
        {poi_data}
        """
        
        # Chain of Thought prompting
        prompt = f"""
        You are analyzing a property listing to evaluate its transportation options and amenities.
        Let's think step by step to thoroughly analyze this property.
        
        First, let's carefully examine the property description and points of interest data to extract key transportation information:
        
        1. **Public Transportation:**
           * Identify all mentioned public transportation options (bus, subway, train, etc.)
           * Note the proximity/distance to these options
           * Assess the frequency or convenience of these options if mentioned
           * If no public transportation is mentioned, note this as a potential limitation
        
        2. **Parking Situation:**
           * Determine if private parking is available (look for mentions of garage, driveway, parking)
           * Note if there are alternative parking options nearby
           * Assess if parking is included or requires additional fees
           * If parking is not mentioned, mark it as uncertain
        
        3. **Walkability:**
           * Identify walking distances to key amenities from the POI data
           * Note if the area is described as walkable
           * Consider proximity to grocery stores, restaurants, etc.
           * Use the POI distances to evaluate walkability (under 0.5 miles is very walkable)
        
        4. **Overall Transportation Assessment:**
           * Evaluate the overall transportation convenience of this property
           * Consider both public and private transportation options
           * Assess how well it meets typical transportation needs
           * Use the nearby POI counts and distances as signals of convenience
        
        Now, let's match these transportation features with the user's preferences: {', '.join(user_preferences)}
        
        5. **Preference Matching:**
           * For each preference, determine if it's satisfied by the property
           * Note which preferences are well-matched and which are lacking
           * Provide clear reasoning for each match or mismatch
        
        6. **Transportation Pros and Cons:**
           * List specific transportation advantages of this property
           * List specific transportation limitations or disadvantages
           * Consider both explicit information and what can be inferred from POI data
        
        Based on this thorough analysis, format your response as a JSON object with the following structure:
        
        {{
            "transportation_features": {{
                "public_transport_available": true/false,
                "transport_types": ["bus", "subway", etc.],
                "distances": {{"downtown": "X mins", "nearest_station": "Y mins"}},
                "parking_available": true/false,
                "walkability_score": 1-10,
                "transportation_convenience_score": 1-10
            }},
            "nearby_amenities": ["gym", "supermarket", etc.],
            "matched_preferences": ["preference1", "preference2"],
            "missing_preferences": ["preference3", "preference4"],
            "transport_sentiment": "positive/negative/neutral",
            "transportation_summary": "A brief summary of transportation options",
            "transportation_pros": ["Pro 1", "Pro 2", "Pro 3"],
            "transportation_cons": ["Con 1", "Con 2", "Con 3"]
        }}
        
        Property details to analyze:
        {combined_description}
        """
        
        # Get response from LLM using SQL-based approach
        try:
            response = self.complete_llm(prompt)
            
            # Parse JSON response
            try:
                # First try direct parsing
                result = json.loads(response)
                return result
            except json.JSONDecodeError:
                # If that fails, try to extract JSON using regex
                json_pattern = r'```json\s*([\s\S]*?)\s*```|{[\s\S]*}'
                match = re.search(json_pattern, response)
                if match:
                    json_str = match.group(1) if match.group(1) else match.group(0)
                    result = json.loads(json_str)
                    return result
                else:
                    print("Error: Could not extract JSON from LLM response.")
                    print("Raw response:", response[:200] + "..." if len(response) > 200 else response)
                    return self._fallback_processing(combined_description, user_preferences)
        except Exception as e:
            print(f"Error during LLM analysis: {e}")
            return self._fallback_processing(combined_description, user_preferences)
    
    def _fallback_processing(self, property_description: str, user_preferences: List[str]) -> Dict[str, Any]:
        """Simple rule-based fallback when LLM processing fails."""
        # Default structure
        result = {
            "transportation_features": {
                "public_transport_available": False,
                "transport_types": [],
                "distances": {},
                "parking_available": False,
                "walkability_score": 5,
                "transportation_convenience_score": 5
            },
            "nearby_amenities": [],
            "matched_preferences": [],
            "missing_preferences": user_preferences.copy(),
            "transport_sentiment": "neutral",
            "transportation_summary": "Could not analyze transportation options.",
            "transportation_pros": [],
            "transportation_cons": ["Analysis failed, limited information available."]
        }
        
        # Simple keyword matching for public transportation
        transport_keywords = ["bus", "subway", "train", "metro", "transit", "station", "T stop", "MBTA"]
        for keyword in transport_keywords:
            if keyword.lower() in property_description.lower():
                result["transportation_features"]["public_transport_available"] = True
                result["transportation_features"]["transport_types"].append(keyword)
        
        # Simple keyword matching for amenities
        amenity_keywords = {
            "gym": ["gym", "fitness", "workout"],
            "supermarket": ["supermarket", "grocery", "market", "store"],
            "restaurant": ["restaurant", "dining", "food"],
            "parking": ["parking", "garage", "driveway"]
        }
        
        for amenity, keywords in amenity_keywords.items():
            for keyword in keywords:
                if keyword.lower() in property_description.lower():
                    result["nearby_amenities"].append(amenity)
                    if amenity in user_preferences:
                        result["matched_preferences"].append(amenity)
                        result["missing_preferences"].remove(amenity)
                    break
        
        # Check for parking
        if "parking" in result["nearby_amenities"] or "garage" in property_description.lower():
            result["transportation_features"]["parking_available"] = True
            result["transportation_pros"].append("Parking is available")
        else:
            result["transportation_cons"].append("No parking mentioned")
        
        # Generate simple summary
        if result["transportation_features"]["public_transport_available"]:
            result["transportation_summary"] = f"Property has access to {', '.join(set(result['transportation_features']['transport_types']))}."
            result["transportation_pros"].append("Public transportation is available")
            result["transport_sentiment"] = "positive"
        else:
            result["transportation_summary"] = "No public transportation mentioned in the description."
            result["transportation_cons"].append("No public transportation mentioned")
            result["transport_sentiment"] = "negative"
        
        return result
    
    def get_property_by_id(self, property_id: int) -> Dict[str, Any]:
        """
        Fetch property details from Snowflake.
        
        Args:
            property_id: Unique identifier for the property (integer)
            
        Returns:
            Dictionary with property details
        """
        try:
            query = f"""
            SELECT *
            FROM LISTINGS.PUBLIC.PROPERTIES_WITH_EMBEDDING
            WHERE property_id = {property_id}
            LIMIT 1
            """
            
            result = self.session.sql(query).collect()
            
            if not result:
                print(f"Warning: No property found with ID {property_id}")
                return {}
            
            # Get column names
            columns_query = "DESCRIBE TABLE LISTINGS.PUBLIC.PROPERTIES_WITH_EMBEDDING"
            columns_result = self.session.sql(columns_query).collect()
            column_names = [row[0] for row in columns_result]
            
            # Convert row to dictionary
            property_details = {column_names[i]: result[0][i] for i in range(min(len(column_names), len(result[0])))}
            
            # Handle None/NULL values
            for key, value in property_details.items():
                if value is None:
                    property_details[key] = ""
            
            return property_details
            
        except Exception as e:
            print(f"Error fetching property details: {e}")
            return {}
    
    def search_similar_properties(self, query: str, top_n: int = 5) -> List[Dict[str, Any]]:
        """
        Search for properties similar to the query using vector similarity.
        
        Args:
            query: The search query
            top_n: Number of results to return
            
        Returns:
            List of property details dictionaries
        """
        try:
            # First check if the embedding column exists
            columns_query = "DESCRIBE TABLE LISTINGS.PUBLIC.PROPERTIES_WITH_EMBEDDING"
            columns_result = self.session.sql(columns_query).collect()
            column_names = [row[0].upper() for row in columns_result]
            
            # Look for an embedding column
            embedding_col = None
            for col in column_names:
                if "EMBEDDING" in col or "VECTOR" in col:
                    embedding_col = col
                    break
                    
            if not embedding_col:
                print("Warning: Could not find vector embedding column. Using text search fallback.")
                # Fallback to simple text search
                fallback_query = f"""
                SELECT *
                FROM LISTINGS.PUBLIC.PROPERTIES_WITH_EMBEDDING
                WHERE CONTAINS(text, '{query}')
                LIMIT {top_n}
                """
                
                result = self.session.sql(fallback_query).collect()
            else:
                # Escape single quotes for SQL
                safe_query = query.replace("'", "''")
                
                # Get dimensionality of the embedding column
                embedding_dim = 1024  # Assuming 1024-dimension by default
                
                # Use the embedding column for vector search
                vector_query = f"""
                SELECT *,
                    VECTOR_COSINE_SIMILARITY(
                        {embedding_col}, 
                        SNOWFLAKE.CORTEX.EMBED_TEXT_1024('snowflake-arctic-embed-l-v2.0', '{safe_query}')
                    ) AS similarity
                FROM LISTINGS.PUBLIC.PROPERTIES_WITH_EMBEDDING
                WHERE {embedding_col} IS NOT NULL
                ORDER BY similarity DESC
                LIMIT {top_n}
                """
                
                result = self.session.sql(vector_query).collect()
            
            if not result:
                return []
                
            # Get column names
            column_names = [row[0] for row in self.session.sql(columns_query).collect()]
            
            # Convert results to list of dictionaries
            properties = []
            for row in result:
                # For vector search results, we have one extra column (similarity)
                if embedding_col:
                    property_dict = {column_names[i]: row[i] for i in range(len(column_names))}
                    property_dict["SIMILARITY"] = row[-1]  # Last column is similarity
                else:
                    property_dict = {column_names[i]: row[i] for i in range(min(len(column_names), len(row)))}
                
                properties.append(property_dict)
                
            return properties
            
        except Exception as e:
            print(f"Error searching for similar properties: {e}")
            print(f"Details: {str(e)}")
            return []
    
    def analyze_property_by_id(self, property_id: int, user_preferences: List[str]) -> Dict[str, Any]:
        """
        Analyze a property using its ID from Snowflake.
        
        Args:
            property_id: Unique identifier for the property (integer)
            user_preferences: List of user preference strings
            
        Returns:
            Analysis result including transportation features, pros, and cons
        """
        print(f"Analyzing property {property_id}...")
        
        # Fetch property details from Snowflake
        property_details = self.get_property_by_id(property_id)
        
        if not property_details:
            print(f"Error: Property with ID {property_id} not found.")
            return {
                "error": f"Property not found: {property_id}"
            }
        
        # Perform analysis
        result = self.analyze_property_cot(property_details, user_preferences)
        
        # Add property metadata
        result["property_id"] = property_id
        result["property_url"] = property_details.get('property_url', '')
        result["address"] = f"{property_details.get('full_street_line', '')} {property_details.get('city', '')}, {property_details.get('state', '')} {property_details.get('zip_code', '')}"
        result["list_price"] = property_details.get('list_price', '')
        result["bedrooms"] = property_details.get('beds', '')
        result["bathrooms"] = property_details.get('full_baths', '')
        
        # Print analysis summary
        self._print_analysis_summary(result)
        
        return result
    
    def batch_analyze_properties(self, property_ids: List[int], user_preferences: List[str]) -> List[Dict[str, Any]]:
        """
        Analyze multiple properties by their IDs.
        
        Args:
            property_ids: List of property IDs (integers)
            user_preferences: List of user preference strings
            
        Returns:
            List of analysis results
        """
        results = []
        
        # Track stats for reporting
        total = len(property_ids)
        errors = 0
        
        print(f"Analyzing {total} properties...")
        
        # Process each property
        for idx, property_id in enumerate(property_ids):
            try:
                # Perform analysis
                print(f"Property {idx+1}/{total}: Analyzing {property_id}...")
                result = self.analyze_property_by_id(property_id, user_preferences)
                
                # Add to results
                results.append(result)
                
            except Exception as e:
                print(f"Error analyzing property {property_id}: {e}")
                results.append({
                    "property_id": property_id,
                    "error": str(e)
                })
                errors += 1
        
        # Print summary
        print(f"\nAnalysis complete:")
        print(f"  Total properties: {total}")
        print(f"  Errors: {errors}")
        
        return results
    
    def find_and_analyze_properties(self, search_query: str, user_preferences: List[str], top_n: int = 5) -> List[Dict[str, Any]]:
        """
        Find properties using vector search and analyze them.
        
        Args:
            search_query: The search query
            user_preferences: List of user preference strings
            top_n: Number of properties to find and analyze
            
        Returns:
            List of analysis results
        """
        # Find similar properties using vector search
        print(f"Searching for properties matching: '{search_query}'")
        similar_properties = self.search_similar_properties(search_query, top_n=top_n)
        
        if not similar_properties:
            print("No matching properties found.")
            return []
        
        print(f"Found {len(similar_properties)} matching properties.")
        
        # Extract property IDs
        property_ids = [p.get("PROPERTY_ID") for p in similar_properties]
        
        # Analyze each property
        analysis_results = self.batch_analyze_properties(property_ids, user_preferences)
        
        # Combine with similarity scores
        for idx, result in enumerate(analysis_results):
            if idx < len(similar_properties):
                result["similarity_score"] = similar_properties[idx].get("SIMILARITY")
        
        # Sort by similarity score
        analysis_results.sort(key=lambda x: x.get("similarity_score", 0), reverse=True)
        
        return analysis_results
    
    def _print_analysis_summary(self, result: Dict[str, Any]) -> None:
        """Print a summary of the analysis results."""
        print("\n=== Transportation Analysis Summary ===")
        
        # Property info
        if 'property_id' in result:
            print(f"Property ID: {result['property_id']}")
        
        if 'property_url' in result:
            print(f"URL: {result['property_url']}")
        
        if 'address' in result:
            print(f"Address: {result['address']}")
        
        if 'list_price' in result:
            print(f"List Price: ${result['list_price']}")
        
        if 'bedrooms' in result and 'bathrooms' in result:
            print(f"Size: {result['bedrooms']} bed, {result['bathrooms']} bath")
        
        # Transportation summary
        print(f"\nTransportation Summary:")
        print(f"  {result.get('transportation_summary', 'No summary available')}")
        
        # Transportation features
        features = result.get('transportation_features', {})
        print("\nTransportation Features:")
        if features.get('public_transport_available', False):
            transport_types = ", ".join(features.get('transport_types', ['unspecified']))
            print(f"  Public Transport: Available ({transport_types})")
        else:
            print("  Public Transport: Not available or not mentioned")
            
        print(f"  Parking: {'Available' if features.get('parking_available', False) else 'Not available or not mentioned'}")
        
        if 'walkability_score' in features:
            print(f"  Walkability Score: {features['walkability_score']}/10")
            
        if 'transportation_convenience_score' in features:
            print(f"  Transportation Convenience: {features['transportation_convenience_score']}/10")
        
        # Preferences matching
        print("\nPreference Matching:")
        matched = result.get('matched_preferences', [])
        missing = result.get('missing_preferences', [])
        
        print("  Matched Preferences:")
        if matched:
            for pref in matched:
                print(f"    ✓ {pref}")
        else:
            print("    None")
            
        print("  Missing Preferences:")
        if missing:
            for pref in missing:
                print(f"    ✗ {pref}")
        else:
            print("    None")
        
        # Pros and cons
        print("\nTransportation Pros:")
        pros = result.get('transportation_pros', [])
        if pros:
            for pro in pros:
                print(f"  + {pro}")
        else:
            print("  None identified")
            
        print("\nTransportation Cons:")
        cons = result.get('transportation_cons', [])
        if cons:
            for con in cons:
                print(f"  - {con}")
        else:
            print("  None identified")
        
        print("\n" + "="*40 + "\n")


# Working with the sample set
def test_with_sample_data():
    """Test the TransportationPro implementation with sample data from CSV."""
    
    # Initialize analyzer
    analyzer = TransportationProAnalyzer(model_name="claude-3-5-sonnet")
    
    # Define user preferences
    user_preferences = ["parking", "supermarket", "public_transport"]
    
    # Get sample property IDs from the database
    try:
        property_id_query = """
        SELECT property_id FROM LISTINGS.PUBLIC.PROPERTIES_WITH_EMBEDDING
        LIMIT 3
        """
        
        property_id_result = analyzer.session.sql(property_id_query).collect()
        
        if not property_id_result:
            print("No properties found in database. Using sample property IDs.")
            # Use sample property IDs from the CSV
            property_ids = [12345, 67890, 13579]  # Replace with actual IDs from your sample
        else:
            property_ids = [row[0] for row in property_id_result]
            print(f"Using property IDs from database: {property_ids}")
        
        # Analyze a few sample properties
        results = analyzer.batch_analyze_properties(
            property_ids=property_ids,
            user_preferences=user_preferences
        )
        
        # Also try a search query
        search_query = "near public transportation with restaurants nearby"
        search_results = analyzer.find_and_analyze_properties(
            search_query=search_query,
            user_preferences=user_preferences,
            top_n=2
        )
        
        return {
            "batch_results": results,
            "search_results": search_results
        }
        
    except Exception as e:
        print(f"Error in sample test: {e}")
        return {"error": str(e)}


if __name__ == "__main__":
    # Test with sample data
    test_results = test_with_sample_data()