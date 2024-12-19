import streamlit as st
from streamlit_folium import st_folium
import folium
import altair as alt
import pandas as pd
import geopandas as gpd
import zipfile
import requests
import io
import os

# Load the datasets
# URL of the zip file on GitHub 
zip_file_url = "https://raw.githubusercontent.com/aminrad404/tlc_nyc/main/pickup+dopoff_cleaned.zip"
# Download the zip file
response = requests.get(zip_file_url)
if response.status_code == 200:
    # Open the zip file
    with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
        # Extract and read the CSV file
        with zip_ref.open('yourfile.csv') as file:  # Replace 'yourfile.csv' with the actual file name
            csv_data = pd.read_csv(file)
            
# Define the raw URLs of the shapefiles
shapefiles = {
    "shp": "https://raw.githubusercontent.com/aminrad404/tlc_nyc/main/taxi_zones.shp",
    "shx": "https://raw.githubusercontent.com/aminrad404/tlc_nyc/main/taxi_zones.shx",
    "dbf": "https://raw.githubusercontent.com/aminrad404/tlc_nyc/main/taxi_zones.dbf",
}

# Create a temporary directory to store the files
temp_dir = "temp_shapefiles"
os.makedirs(temp_dir, exist_ok=True)

# Download each file
for ext, url in shapefiles.items():
    response = requests.get(url)
    if response.status_code == 200:
        with open(os.path.join(temp_dir, f"taxi_zones.{ext}"), "wb") as file:
            file.write(response.content)
    else:
        print(f"Failed to download {ext} file.")

# Load the shapefile into Geopandas
shapefile_path = os.path.join(temp_dir, "taxi_zones.shp")
gdf = gpd.read_file(shapefile_path)


# Ensure correct data types for merging
csv_data['PULocationID'] = csv_data['PULocationID'].astype(int)
csv_data['DOLocationID'] = csv_data['DOLocationID'].astype(int)
shapefile['LocationID'] = shapefile['LocationID'].astype(int)

# Merge for pickup locations
pickup_gdf = csv_data.merge(
    shapefile[['LocationID', 'zone', 'borough']],
    left_on='PULocationID',
    right_on='LocationID',
    how='left'
).rename(columns={
    'zone': 'pickup_zone',
    'borough': 'pickup_borough'
}).drop(columns=['LocationID'])

# Merge for dropoff locations
dropoff_gdf = pickup_gdf.merge(
    shapefile[['LocationID', 'zone', 'borough', 'geometry']],
    left_on='DOLocationID',
    right_on='LocationID',
    how='left'
).rename(columns={
    'zone': 'dropoff_zone',
    'borough': 'dropoff_borough',
    'geometry': 'dropoff_geometry'
}).drop(columns=['LocationID'])

# Convert to GeoDataFrame for folium mapping
gdf = gpd.GeoDataFrame(
    dropoff_gdf,
    geometry='dropoff_geometry',
    crs=shapefile.crs
)

# Ensure geometry for pickup is retained if needed
pickup_geometry = shapefile[['LocationID', 'geometry']].rename(columns={'geometry': 'pickup_geometry'})
gdf = gdf.merge(
    pickup_geometry,
    left_on='PULocationID',
    right_on='LocationID',
    how='left'
).drop(columns=['LocationID'])

# Final GeoDataFrame for mapping
gdf = gpd.GeoDataFrame(
    gdf,
    geometry='dropoff_geometry',  # Set dropoff as the main geometry
    crs=shapefile.crs
)

# Sidebar Filters
st.sidebar.title("Filter Parameters")
# Clean and prepare unique values for dropdowns
gdf['pickup_zone'] = gdf['pickup_zone'].fillna('Unknown').astype(str)
gdf['dropoff_zone'] = gdf['dropoff_zone'].fillna('Unknown').astype(str)

pickup_location = st.sidebar.selectbox('Pickup Location', ['All'] + sorted(list(gdf['pickup_zone'].unique())))
dropoff_location = st.sidebar.selectbox('Dropoff Location', ['All'] + sorted(list(gdf['dropoff_zone'].unique())))

# Define the correct order for months, days, and time periods
correct_month_order = ['January', 'February', 'March', 'April', 'May', 'June',
                       'July', 'August', 'September', 'October', 'November', 'December']
correct_day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
correct_time_order = ['night_time', 'am_rush', 'day_time', 'pm_rush']

# Sort unique values based on the defined order
sorted_months = sorted(gdf['month'].dropna().unique(), key=lambda x: correct_month_order.index(x))
sorted_days = sorted(gdf['day_pickup'].dropna().unique(), key=lambda x: correct_day_order.index(x))
sorted_time_order = sorted(gdf['pickup_period'].dropna().unique(), key=lambda x: correct_time_order.index(x))

month = st.sidebar.selectbox('Month', ['All'] + sorted_months)
day = st.sidebar.selectbox('Day', ['All'] + sorted_days)
time_period = st.sidebar.selectbox('Time Period', ['All'] + sorted_time_order)


# Filter and Aggregate Data
@st.cache_data
@st.cache_data
def filter_and_aggregate(_data, month, day, time_period, pickup_location, dropoff_location):
    filtered_data = _data.copy()

    # Apply filters for pickup and dropoff locations
    if pickup_location != 'All':
        filtered_data = filtered_data[filtered_data['pickup_zone'] == pickup_location]
    if dropoff_location != 'All':
        filtered_data = filtered_data[filtered_data['dropoff_zone'] == dropoff_location]

    # Apply filters for month, day, and time period
    if month != 'All':
        filtered_data = filtered_data[filtered_data['month'] == month]
    if day != 'All':
        filtered_data = filtered_data[filtered_data['day_pickup'] == day]
    if time_period != 'All':
        filtered_data = filtered_data[filtered_data['pickup_period'] == time_period]

    # Aggregate by Pickup or Dropoff LocationID
    if pickup_location == 'All':
        aggregated_data = filtered_data.groupby('PULocationID').agg({
            'pickup_zone': 'first',
            'pickup_borough': 'first',
            'trip_count': 'sum',
            'avg_trip_duration': lambda x: round(x.mean(), 0),
            'avg_trip_distance': lambda x: round(x.mean(), 2),
            'pickup_geometry': 'first'  # Ensure geometry is retained
        }).reset_index()
        aggregated_data.rename(columns={
            'PULocationID': 'LocationID',
            'pickup_zone': 'zone',
            'pickup_borough': 'borough',
            'pickup_geometry': 'geometry'  # Rename geometry column
        }, inplace=True)
    else:
        aggregated_data = filtered_data.groupby('DOLocationID').agg({
            'dropoff_zone': 'first',
            'dropoff_borough': 'first',
            'trip_count': 'sum',
            'avg_trip_duration': lambda x: round(x.mean(), 0),
            'avg_trip_distance': lambda x: round(x.mean(), 2),
            'dropoff_geometry': 'first'  # Ensure geometry is retained
        }).reset_index()
        aggregated_data.rename(columns={
            'DOLocationID': 'LocationID',
            'dropoff_zone': 'zone',
            'dropoff_borough': 'borough',
            'dropoff_geometry': 'geometry'  # Rename geometry column
        }, inplace=True)

    # Convert to GeoDataFrame
    aggregated_data = gpd.GeoDataFrame(aggregated_data, geometry='geometry', crs=_data.crs)
    return aggregated_data



aggregated_data = filter_and_aggregate(
    gdf,
    month,
    day,
    time_period,
    pickup_location,
    dropoff_location
)

if aggregated_data.empty:
    st.warning("No data available for the selected filters.")
    st.stop()

# Top 5 Zones based on Trip Count
top_5_zones = aggregated_data.sort_values(by='trip_count', ascending=False).head(5)



# Display the Top 5 Table
st.sidebar.title("Top 5 Zones by Trips")
top_5_table = top_5_zones.rename(columns={
    'LocationID': 'Location ID',
    'zone': 'Zone',
    'borough': 'Borough',
    'trip_count': 'Number of Trips',
    'avg_trip_duration': 'Avg Duration (min)',
    'avg_trip_distance': 'Avg Distance (miles)'
})

# Format the table to remove trailing zeros
top_5_table['Avg Duration (min)'] = top_5_table['Avg Duration (min)'].astype(int)
top_5_table['Avg Distance (miles)'] = top_5_table['Avg Distance (miles)'].map('{:.2f}'.format)

# Calculate the maximum value for 'Number of Trips'
max_trip_count = top_5_table['Number of Trips'].max()

# Display the table in the sidebar
st.sidebar.table(
    top_5_table[['Location ID', 'Zone', 'Borough', 'Number of Trips', 'Avg Duration (min)', 'Avg Distance (miles)']]
)


# Bar Chart for Top 5 Zones
bar_chart = alt.Chart(top_5_table).mark_bar().encode(
    x=alt.X('Number of Trips:Q', title='Number of Trips'),
    y=alt.Y('Zone:N', sort='-x', title='Zone Name'),
    color=alt.Color(
        'Number of Trips:Q',
        scale=alt.Scale(scheme='viridis', domain=[0, max_trip_count]),
        title='Trip Counts'
    )
).properties(
    width=600,
    height=400
)

st.altair_chart(bar_chart)

# Initialize the Folium map
m = folium.Map(
    location=[40.7685, -73.9822],
    zoom_start=10,
    tiles="CartoDB positron"
)

# Add GeoJSON with filtered data and style
folium.Choropleth(
    geo_data=aggregated_data,
    data=aggregated_data,
    columns=['LocationID', 'trip_count'],
    key_on='feature.properties.LocationID',
    fill_color='viridis',
    nan_fill_color='rgba(0, 0, 0, 0)',
    fill_opacity=0.7,
    line_opacity=0.2,
    legend_name='Number of Trips'
).add_to(m)

# Add tooltips
folium.GeoJson(
    aggregated_data,
    style_function=lambda x: {'fillColor': 'transparent', 'color': 'black', 'weight': 0.3},
    tooltip=folium.GeoJsonTooltip(
        fields=['LocationID', 'zone', 'trip_count', 'borough'],
        aliases=['Location ID', 'Zone:', 'Number of Trips:', 'Borough:']
    )
).add_to(m)

# Highlight the selected pickup location in RED
if pickup_location != 'All' and dropoff_location != 'All':
    selected_zone = shapefile[shapefile['zone'] == pickup_location]

    # Add the selected pickup location as a separate layer
    folium.GeoJson(
        selected_zone,
        name="Selected Pickup Zone",
        style_function=lambda x: {
            'fillColor': 'red',
            'color': 'black',
            'weight': 2,
            'fillOpacity': 0.6
        },
        highlight_function=lambda x: {
            'weight': 4,
            'color': 'black'
        },
        tooltip=folium.GeoJsonTooltip(
            fields=['LocationID', 'zone', 'borough'],
            aliases=['Location ID', 'Zone:','Borough:']
        )
    ).add_to(m)

# Add LayerControl to allow toggling layers
folium.LayerControl().add_to(m)

# Streamlit Folium Integration
st.title("Taxi Trip Choropleth Map")
st_folium(m, width=600, height=500)
