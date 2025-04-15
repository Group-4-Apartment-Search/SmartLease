import csv
import requests
import os
import sys
import json
from math import radians, cos, sin, asin, sqrt
import dotenv

# Load environment variables from .env file
dotenv.load_dotenv()

# POI types to search for
DEFAULT_POI_TYPES = ['restaurant', 'cafe', 'hospital', 'pharmacy', 'atm', 'bank']

def find_nearby_pois(csv_file, api_key, poi_types=None, radius=1000, output_file=None, max_properties=3):
    """
    Find nearby POIs for properties listed in a CSV file.
    
    Args:
        csv_file (str): Path to the CSV file with property information
        api_key (str): Google Maps API key
        poi_types (list): Types of POIs to search for
        radius (int): Search radius in meters
        output_file (str): Optional file to save results (can be .json or .csv)
        max_properties (int): Maximum number of properties to process
    """
    if poi_types is None:
        poi_types = DEFAULT_POI_TYPES
    
    # Check if API key is provided
    if not api_key:
        print("Error: Google Maps API key is required.")
        return
    
    # Check if file exists
    if not os.path.exists(csv_file):
        print(f"Error: File '{csv_file}' not found.")
        return
    
    # Read the entire CSV file into memory to preserve all columns
    all_rows = []
    property_data = {}
    
    with open(csv_file, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        field_names = reader.fieldnames.copy()
        
        for row in reader:
            all_rows.append(row)
    
    # Process properties (limited by max_properties)
    property_count = 0
    
    for row in all_rows:
        if property_count >= max_properties:
            print(f"\nReached limit of {max_properties} properties. Stopping processing.")
            break
            
        property_id = row.get('property_id', 'unknown')
        
        # Check if we have location data
        if 'latitude' in row and 'longitude' in row and row['latitude'] and row['longitude']:
            lat = float(row['latitude'])
            lng = float(row['longitude'])
            address = get_address_from_row(row)
            
            print(f"Processing: {address} (ID: {property_id})")
            
            # Get POIs for each type
            property_results = {}
            for poi_type in poi_types:
                places = get_nearby_places(lat, lng, poi_type, api_key, radius)
                property_results[poi_type] = places
            
            # Store processed data
            property_data[property_id] = {
                'address': address,
                'lat': lat,
                'lng': lng,
                'pois': property_results
            }
            
            property_count += 1
        else:
            # If no coordinates, try to geocode the address
            address = get_address_from_row(row)
            
            if address:
                print(f"Geocoding address: {address} (ID: {property_id})")
                
                # Get coordinates from address
                coords = geocode_address(address, api_key)
                if coords:
                    lat, lng = coords
                    
                    # Get POIs for each type
                    property_results = {}
                    for poi_type in poi_types:
                        places = get_nearby_places(lat, lng, poi_type, api_key, radius)
                        property_results[poi_type] = places
                    
                    # Store processed data
                    property_data[property_id] = {
                        'address': address,
                        'lat': lat,
                        'lng': lng,
                        'pois': property_results
                    }
                    
                    property_count += 1
                else:
                    print(f"  Could not geocode address: {address}")
                    if property_count < max_properties:
                        property_data[property_id] = {
                            'address': address,
                            'error': "Could not geocode address"
                        }
                        property_count += 1
            else:
                print(f"  No address found for property ID: {property_id}")
    
    # Print results for the processed properties
    print_results(property_data)
    
    # Create new column headers for POI data
    new_fields = []
    for poi_type in poi_types:
        new_fields.extend([
            f'{poi_type}_count',
            f'closest_{poi_type}_name',
            f'closest_{poi_type}_distance',
            f'closest_{poi_type}_rating'
        ])
    
    # Add new fields to the header
    extended_field_names = field_names + new_fields
    
    # Create output file (append _with_pois to original filename)
    if not output_file:
        base_name = os.path.splitext(csv_file)[0]
        output_file = f"{base_name}_with_pois.csv"
    
    # Write out the extended CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=extended_field_names)
        writer.writeheader()
        
        # Write each row
        for row in all_rows:
            property_id = row.get('property_id', 'unknown')
            extended_row = row.copy()
            
            # Add POI data if we have it for this property
            if property_id in property_data:
                prop_data = property_data[property_id]
                
                if 'pois' in prop_data:
                    for poi_type in poi_types:
                        places = prop_data['pois'].get(poi_type, [])
                        extended_row[f'{poi_type}_count'] = len(places)
                        
                        # Add closest POI info if available
                        if places:
                            closest = places[0]  # Places are already sorted by distance
                            extended_row[f'closest_{poi_type}_name'] = closest['name']
                            extended_row[f'closest_{poi_type}_distance'] = f"{closest['distance']:.0f}"
                            extended_row[f'closest_{poi_type}_rating'] = closest['rating']
                        else:
                            extended_row[f'closest_{poi_type}_name'] = ''
                            extended_row[f'closest_{poi_type}_distance'] = ''
                            extended_row[f'closest_{poi_type}_rating'] = ''
                elif 'error' in prop_data:
                    # Fill with error information
                    for poi_type in poi_types:
                        extended_row[f'{poi_type}_count'] = 'ERROR'
                        extended_row[f'closest_{poi_type}_name'] = prop_data['error']
                        extended_row[f'closest_{poi_type}_distance'] = ''
                        extended_row[f'closest_{poi_type}_rating'] = ''
            else:
                # This property wasn't processed, leave POI fields blank
                for field in new_fields:
                    extended_row[field] = ''
            
            writer.writerow(extended_row)
    
    print(f"\nExtended CSV with POI data saved to: {output_file}")
    return property_data

def get_address_from_row(row):
    """Extract the complete address from a CSV row"""
    # Try to use full_street_line if available
    if 'full_street_line' in row and row['full_street_line']:
        street = row['full_street_line']
    # Otherwise construct from street and unit
    elif 'street' in row and row['street']:
        street = row['street']
        if 'unit' in row and row['unit']:
            street += f" {row['unit']}"
    else:
        street = ""
    
    # Build the complete address
    address_parts = []
    if street:
        address_parts.append(street)
    if 'city' in row and row['city']:
        address_parts.append(row['city'])
    if 'state' in row and row['state']:
        address_parts.append(row['state'])
    if 'zip_code' in row and row['zip_code']:
        address_parts.append(row['zip_code'])
    
    return ", ".join(address_parts) if address_parts else None

def geocode_address(address, api_key):
    """Convert an address to latitude and longitude"""
    url = f"https://maps.googleapis.com/maps/api/geocode/json?address={address}&key={api_key}"
    
    try:
        response = requests.get(url)
        data = response.json()
        
        if data['status'] == 'OK':
            location = data['results'][0]['geometry']['location']
            return location['lat'], location['lng']
        else:
            print(f"  Geocoding error: {data['status']}")
            return None
    except Exception as e:
        print(f"  Geocoding exception: {str(e)}")
        return None

def get_nearby_places(lat, lng, place_type, api_key, radius=1000):
    """Find nearby places of a specific type"""
    url = (
        f"https://maps.googleapis.com/maps/api/place/nearbysearch/json?"
        f"location={lat},{lng}&radius={radius}&type={place_type}&key={api_key}"
    )
    
    try:
        response = requests.get(url)
        data = response.json()
        
        places = []
        if data['status'] == 'OK':
            for place in data['results']:
                # Calculate distance
                place_lat = place['geometry']['location']['lat']
                place_lng = place['geometry']['location']['lng']
                distance = calculate_distance(lat, lng, place_lat, place_lng)
                
                # Create place entry
                places.append({
                    'name': place['name'],
                    'vicinity': place.get('vicinity', 'No address'),
                    'rating': place.get('rating', 'No rating'),
                    'distance': distance
                })
                
            # Sort by distance
            places.sort(key=lambda x: x['distance'])
        
        return places
    except Exception as e:
        print(f"  Error finding {place_type}s: {str(e)}")
        return []

def calculate_distance(lat1, lon1, lat2, lon2):
    """Calculate distance between two points in meters using the Haversine formula"""
    # Convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    
    # Haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371000  # Radius of earth in meters
    
    return c * r

def print_results(results):
    """Print results in a readable format"""
    for property_id, property_data in results.items():
        address = property_data.get('address', 'Unknown address')
        print(f"\n===== Property ID: {property_id} =====")
        print(f"Address: {address}")
        
        if "error" in property_data:
            print(f"ERROR: {property_data['error']}")
            continue
        
        pois = property_data.get('pois', {})
        for poi_type, places in pois.items():
            print(f"\n== Nearby {poi_type}s: {len(places)} found ==")
            
            # Show top 5 only to keep output manageable
            for i, place in enumerate(places[:5], 1):
                print(f"{i}. {place['name']}")
                print(f"   Address: {place['vicinity']}")
                print(f"   Rating: {place['rating']}")
                print(f"   Distance: {place['distance']:.0f} meters")
            
            if len(places) > 5:
                print(f"... and {len(places) - 5} more")

def save_results_to_csv(results, output_file):
    """Save results to a CSV file"""
    # Create headers for the CSV file
    headers = ['property_id', 'address', 'latitude', 'longitude']
    
    # Add POI type columns (we'll store counts and closest POI for each type)
    for poi_type in DEFAULT_POI_TYPES:
        headers.extend([
            f'{poi_type}_count',
            f'closest_{poi_type}_name',
            f'closest_{poi_type}_distance',
            f'closest_{poi_type}_rating'
        ])
    
    # Create CSV rows
    rows = []
    for property_id, property_data in results.items():
        row = {
            'property_id': property_id,
            'address': property_data.get('address', ''),
            'latitude': property_data.get('lat', ''),
            'longitude': property_data.get('lng', '')
        }
        
        # Add POI data
        if 'pois' in property_data:
            for poi_type in DEFAULT_POI_TYPES:
                places = property_data['pois'].get(poi_type, [])
                row[f'{poi_type}_count'] = len(places)
                
                # Add closest POI info if available
                if places:
                    closest = places[0]  # Places are already sorted by distance
                    row[f'closest_{poi_type}_name'] = closest['name']
                    row[f'closest_{poi_type}_distance'] = f"{closest['distance']:.0f}"
                    row[f'closest_{poi_type}_rating'] = closest['rating']
                else:
                    row[f'closest_{poi_type}_name'] = ''
                    row[f'closest_{poi_type}_distance'] = ''
                    row[f'closest_{poi_type}_rating'] = ''
        
        rows.append(row)
    
    # Write to CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)

def main():
    """Main function to run when the script is executed directly"""
    # Get API key from environment variables
    api_key = os.environ.get("MAPS_API_KEY")
    
    if not api_key:
        print("Error: MAPS_API_KEY environment variable is not set.")
        print("Please set it using:")
        print("set MAPS_API_KEY=your_google_maps_api_key     # For Windows")
        return
    
    # Hardcoded file paths
    csv_file = "C:/Users/aqeel/work/NEU/SPRING_25/GEN_AI/project/scrape/HomeHarvest.csv"
    
    # Generate output filename based on input filename
    base_name = os.path.splitext(csv_file)[0]
    output_file = f"{base_name}_with_pois.csv"
    
    # Check if file exists
    if not os.path.exists(csv_file):
        print(f"Error: CSV file '{csv_file}' not found.")
        return
    
    print(f"Reading properties from: {csv_file}")
    print(f"Results will be saved to: {output_file}")
    
    # Remove the save_results_to_csv function since we're not using it anymore
    # Run the function with max_properties=3
    find_nearby_pois(csv_file, api_key, output_file=output_file, max_properties=3)

if __name__ == "__main__":
    main()