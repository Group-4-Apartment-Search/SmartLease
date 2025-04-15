import streamlit as st
import pandas as pd
import time
import os
from datetime import datetime
import dotenv

# Load environment variables from .env file
dotenv.load_dotenv()


# Import the functions from your map_functions.py file
from maps import get_nearby_attractions, find_nearby_attractions

def main():
    st.title("Property Nearby Attractions Finder")
    st.write("This app finds attractions near properties using Google Maps API")
    
    # Get API key from environment variables
    api_key = os.environ.get("MAPS_API_KEY")
    
    if not api_key:
        st.error("⚠️ Google Maps API key not found in environment variables!")
        st.info("Please create a .env file in the same directory with the line: GOOGLE_MAPS_API_KEY=your_api_key_here")
        return
    else:
        st.success("✅ Google Maps API key loaded from environment variables")
    
    # Input Methods
    input_method = st.radio(
        "Choose input method:",
        ["Upload CSV file", "Manual property entry"]
    )
    
    if input_method == "Upload CSV file":
        handle_file_upload(api_key)
    else:
        handle_manual_entry(api_key)

def handle_file_upload(api_key):
    st.subheader("Upload Property Data")
    
    uploaded_file = st.file_uploader("Choose a CSV file", type="csv")
    
    if uploaded_file is not None:
        try:
            df = pd.read_csv(uploaded_file)
            st.write("Preview of uploaded data:")
            st.dataframe(df.head())
            
            # Check if required columns exist
            has_coords = 'latitude' in df.columns and 'longitude' in df.columns
            has_address = 'address' in df.columns
            
            if not (has_coords or has_address):
                st.error("CSV must contain either 'latitude' and 'longitude' columns OR an 'address' column")
                return
            
            # Configuration options
            with st.expander("Configuration Options"):
                radius = st.slider("Search radius (meters):", 500, 10000, 3000)
                
                all_types = ["tourist_attraction", "museum", "park", "amusement_park", 
                             "restaurant", "bar", "cafe", "shopping_mall", "zoo", 
                             "aquarium", "art_gallery", "movie_theater"]
                
                attraction_types = st.multiselect(
                    "Select attraction types to search for:",
                    all_types,
                    default=["tourist_attraction", "museum", "park"]
                )
                
                max_results = st.slider("Maximum attractions per property:", 1, 10, 3)
                
                sample_size = st.slider("Number of properties to process (0 for all):", 
                                       0, min(100, len(df)), min(10, len(df)))
                
            if st.button("Find Nearby Attractions"):
                with st.spinner("Searching for attractions..."):
                    # Use a sample of the data if specified
                    if sample_size > 0 and sample_size < len(df):
                        process_df = df.sample(sample_size)
                        st.info(f"Processing {sample_size} randomly selected properties")
                    else:
                        process_df = df
                        
                    # Call the function from your module
                    result_df = get_nearby_attractions(
                        properties_df=process_df,
                        api_key=api_key,
                        radius=radius,
                        attraction_types=attraction_types,
                        max_results=max_results
                    )
                    
                    # Display results
                    st.success("Search complete!")
                    st.subheader("Results:")
                    st.dataframe(result_df)
                    
                    # Download option
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    csv = result_df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        "Download results as CSV",
                        csv,
                        f"property_attractions_{timestamp}.csv",
                        "text/csv",
                        key='download-csv'
                    )
        except Exception as e:
            st.error(f"Error processing file: {str(e)}")

def handle_manual_entry(api_key):
    st.subheader("Enter Property Details")
    
    # Choose input type
    coord_type = st.radio(
        "Input type:",
        ["Address", "Coordinates"]
    )
    
    if coord_type == "Address":
        address = st.text_input("Property Address:")
        has_valid_input = bool(address.strip())
        location = address
        location_type = "address"
    else:
        col1, col2 = st.columns(2)
        with col1:
            latitude = st.number_input("Latitude:", value=40.7128, format="%.6f")
        with col2:
            longitude = st.number_input("Longitude:", value=-74.0060, format="%.6f")
        has_valid_input = True
        location = f"{latitude},{longitude}"
        location_type = "location"
    
    # Configuration options
    radius = st.slider("Search radius (meters):", 500, 10000, 3000)
    
    all_types = ["tourist_attraction", "museum", "park", "amusement_park", 
                 "restaurant", "bar", "cafe", "shopping_mall", "zoo", 
                 "aquarium", "art_gallery", "movie_theater"]
    
    attraction_types = st.multiselect(
        "Select attraction types to search for:",
        all_types,
        default=["tourist_attraction", "museum", "park"]
    )
    
    max_results = st.slider("Maximum attractions to display:", 1, 20, 5)
    
    if st.button("Find Nearby Attractions") and has_valid_input:
        with st.spinner("Searching for attractions..."):
            try:
                # Call the function from your module
                attractions = find_nearby_attractions(
                    location=location,
                    location_type=location_type,
                    api_key=api_key,
                    radius=radius,
                    types=attraction_types,
                    max_results=max_results
                )
                
                if not attractions:
                    st.warning("No attractions found within the specified radius.")
                    return
                
                # Display results
                st.success(f"Found {len(attractions)} attractions!")
                
                # Display in cards
                for i, attraction in enumerate(attractions):
                    with st.container():
                        col1, col2 = st.columns([1, 3])
                        with col1:
                            st.write(f"**#{i+1}**")
                        with col2:
                            st.subheader(attraction['name'])
                            cols = st.columns(3)
                            with cols[0]:
                                st.write(f"**Type:** {attraction['type'].replace('_', ' ').title()}")
                            with cols[1]:
                                if attraction['distance'] is not None:
                                    st.write(f"**Distance:** {attraction['distance']}m")
                            with cols[2]:
                                if attraction['rating'] is not None:
                                    st.write(f"**Rating:** {attraction['rating']}★")
                        st.divider()
                
            except Exception as e:
                st.error(f"Error: {str(e)}")

if __name__ == "__main__":
    st.set_page_config(
        page_title="Property Attractions Finder",
        page_icon="🏙️",
    )
    main()