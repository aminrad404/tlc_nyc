import streamlit as st
from streamlit_folium import st_folium
import folium
from branca.colormap import StepColormap
import pandas as pd
import geopandas as gpd

# Load the dataset
path = r"C:\Users\USAR705244\OneDrive - WSP O365\Documents\Kaggle\TLC 2024\\"

# Load GeoDataFrame and dropoff counts
gdf = gpd.read_file(path + 'taxi_zones.shp')
dropoff_counts = pd.read_csv(path + 'dropoff_cleaned.csv')

missing_ids = set(dropoff_counts['DOLocationID']) - set(gdf['LocationID'])
# Filter out rows with missing LocationIDs
dropoff_counts = dropoff_counts[~dropoff_counts['DOLocationID'].isin(missing_ids)]


# Merge trip data into GeoDataFrame on 'LocationID'
gdf = gdf.merge(dropoff_counts, left_on='LocationID', right_on='DOLocationID', how='left')
gdf = gdf.drop('DOLocationID', axis=1)
gdf.dropna(inplace=True)

# Ensure correct data types
gdf['trip_count'] = gdf['trip_count'].astype(int)
gdf['LocationID'] = gdf['LocationID'].astype(int)

# Sidebar Filters
st.sidebar.title("Filter Parameters")

# Define the correct order for months, days, and time periods
correct_month_order = ['January', 'February', 'March', 'April', 'May', 'June',
                       'July', 'August', 'September', 'October', 'November', 'December']
correct_day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
correct_time_order = ['night_time', 'am_rush', 'day_time', 'pm_rush']

# Sort unique values based on the defined order
sorted_months = sorted(gdf['month'].dropna().unique(), key=lambda x: correct_month_order.index(x))
sorted_days = sorted(gdf['day_dropoff'].dropna().unique(), key=lambda x: correct_day_order.index(x))
sorted_time_order = sorted(gdf['dropoff_period'].dropna().unique(), key=lambda x: correct_time_order.index(x))

month = st.sidebar.selectbox('Month', ['All'] + sorted_months)
day = st.sidebar.selectbox('Day', ['All'] + sorted_days)
time_period = st.sidebar.selectbox('Time Period', ['All'] + sorted_time_order)


# Filter and Aggregate Data
@st.cache_data
def filter_and_aggregate(_data, month, day, time_period):
    filtered_data = _data.copy()
    if month != 'All':
        filtered_data = filtered_data[filtered_data['month'] == month]
    if day != 'All':
        filtered_data = filtered_data[filtered_data['day_dropoff'] == day]
    if time_period != 'All':
        filtered_data = filtered_data[filtered_data['dropoff_period'] == time_period]

    # Aggregate by Station ID
    aggregated_data = filtered_data.groupby('LocationID').agg({
        'zone': 'first',
        'borough': 'first',
        'trip_count': 'sum'
    }).reset_index()  # Ensure index is reset without duplicating 'LocationID'
    aggregated_data = aggregated_data.dropna()

    # Deduplicate LocationID and geometry
    unique_geometries = _data[['LocationID', 'geometry']].drop_duplicates('LocationID')

    # Merge geometries back from the unique data
    aggregated_data = aggregated_data.merge(unique_geometries,
                                            on='LocationID',
                                            how='left')
    # Convert to GeoDataFrame
    aggregated_data = gpd.GeoDataFrame(aggregated_data, geometry='geometry', crs=_data.crs)

    return aggregated_data

aggregated_data = filter_and_aggregate(gdf, month, day, time_period)

if aggregated_data.empty:
    st.warning("No data available for the selected filters.")
    st.stop()

# Top 10 stations based on trip count
top_10_stations = aggregated_data.sort_values(by='trip_count', ascending=False).head(10)

# Display the Top 10 table in the sidebar
st.sidebar.title("Top 10 Stations")
top_10_table = top_10_stations.rename(columns={'LocationID': 'Location ID', 'trip_count': 'Number of Trips','zone':'Zone'})
st.sidebar.table(top_10_table[['Location ID','Zone', 'Number of Trips']])

# Initialize the Folium map
m = folium.Map(
    location=[40.7685,-73.9822],
    zoom_start=10,
    tiles="CartoDB positron"
)

# Add GeoJSON with filtered data and style
folium.Choropleth(
    geo_data=aggregated_data,  # Cleaned GeoDataFrame
    data=aggregated_data,
    columns=['LocationID', 'trip_count'],
    key_on='feature.properties.LocationID',
    fill_color= 'YlOrRd',
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
        fields=['LocationID','zone', 'trip_count', 'borough'],
        aliases=['Location ID','Zone:', 'Number of Trips:', 'Borough:']
    )
).add_to(m)

# Streamlit Folium Integration
st.title("Taxi Trip Dropoff Locations Choropleth Map")
st_folium(m, width=600, height=500)