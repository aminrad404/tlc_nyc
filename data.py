# Amin Rad
#10/12/2024
# Description:  This program will read the data and clean the data



import numpy as np
import pandas as pd

path = r"C:\Users\USAR705244\OneDrive - WSP O365\Documents\Kaggle\TLC 2024\\"

lyft = pd.read_csv(path+'trips.csv')
lyft = lyft.dropna()


lyft = lyft[['tpep_pickup_datetime','tpep_dropoff_datetime',"PULocationID","DOLocationID",'passenger_count','RatecodeID','trip_distance']]

lyft['tpep_pickup_datetime'] = pd.to_datetime(lyft['tpep_pickup_datetime'])
lyft['tpep_dropoff_datetime'] = pd.to_datetime(lyft['tpep_dropoff_datetime'])
lyft['trip_duration'] = (lyft['tpep_dropoff_datetime']-lyft['tpep_pickup_datetime'])/ np.timedelta64(1,'m')

month_order = ['January', 'February', 'March', 'April', 'May', 'June','July']

day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

lyft['hour_pickup'] = lyft['tpep_pickup_datetime'].dt.hour
lyft['day_pickup'] = lyft['tpep_pickup_datetime'].dt.day_name()
lyft['day_pickup'] = pd.Categorical(lyft['day_pickup'], categories=day_order, ordered=True)

lyft['month'] = lyft['tpep_pickup_datetime'].dt.month_name()
lyft['month'] = pd.Categorical(lyft['month'], categories=month_order, ordered=True)

lyft['year'] = lyft['tpep_pickup_datetime'].dt.year
lyft['weekday_pickup'] = lyft['tpep_pickup_datetime'].dt.weekday
lyft['week'] = lyft['tpep_pickup_datetime'].dt.strftime('W%V')

lyft['hour_dropoff'] = lyft['tpep_dropoff_datetime'].dt.hour
lyft['day_dropoff'] = lyft['tpep_dropoff_datetime'].dt.day_name()
lyft['day_dropoff'] = pd.Categorical(lyft['day_dropoff'], categories=day_order, ordered=True)
lyft['weekday_dropoff'] = lyft['tpep_dropoff_datetime'].dt.weekday


# Define a function to check if a given time is during rush hour
def get_time_period(time):
    """
    Divides the day into four time periods based on the hour of the day.

    Parameters:
        hour (int): The hour in 24-hour format (0-23).

    Returns:
        str: The time period (am_rush_hour, day_time, pm_rush_hour, night_time).
    """
    if 6 <= time.hour < 10:
        return 'am_rush'
    elif 10 <= time.hour < 16:
        return 'day_time'
    elif 16 <= time.hour < 19:
        return 'pm_rush'
    else:
        return 'night_time'


lyft['pickup_period'] = lyft['tpep_pickup_datetime'].apply(get_time_period)
lyft['dropoff_period'] = lyft['tpep_dropoff_datetime'].apply(get_time_period)


lyft = lyft.loc[lyft['month']!='December']
lyft = lyft.loc[lyft['week'] != 'W52']
lyft = lyft.loc[lyft['RatecodeID'] != 99]
lyft = lyft.loc[lyft['passenger_count'] != 0]

lyft['pickup_dropoff'] = lyft['PULocationID'].astype(str) + '-' +lyft['DOLocationID'].astype(str)
grouped =  lyft.groupby(by='pickup_dropoff').mean(numeric_only=True)['trip_distance']
grouped_dict = grouped.to_dict()
# 1. Create a mean_distance column that is a copy of the pickup_dropoff helper column
lyft['mean_distance'] = lyft['pickup_dropoff']

# 2. Map `grouped_dict` to the `mean_distance` column
lyft['mean_distance'] = lyft['mean_distance'].map(grouped_dict)

grouped = lyft.groupby(by='pickup_dropoff').mean(numeric_only=True)['trip_duration']
grouped_dict = grouped.to_dict()


# 1. Create a mean_distance column that is a copy of the pickup_dropoff helper column
lyft['mean_duration'] = lyft['pickup_dropoff']

# 2. Map `grouped_dict` to the `mean_distance` column
lyft['mean_duration'] = lyft['mean_duration'].map(grouped_dict)

lyft.to_csv(path_or_buf=path+'lyft_cleaned.csv')

pickup = lyft.drop(columns=['tpep_pickup_datetime', 'tpep_dropoff_datetime','DOLocationID','trip_distance','trip_duration',
                            'hour_dropoff', 'day_dropoff',
                            'weekday_dropoff','dropoff_period'])

dropoff = lyft.drop(columns=['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'PULocationID','trip_distance','trip_duration',
                            'hour_pickup', 'day_pickup',
                            'weekday_pickup','pickup_period'])

# Aggregating the data to get the number of trips from each start station
pickup_counts = (
    pickup.groupby(['PULocationID','month','day_pickup','pickup_period'], as_index=False)
    .size()
    .rename(columns={'size': 'trip_count'})
)
# Fill NaN values in trip_count with 0 (if necessary)
pickup_counts['trip_count'] = pickup_counts['trip_count'].fillna(0)

pickup_counts = pickup_counts[pickup_counts['trip_count'] > 0]

pickup_counts.to_csv(path_or_buf=path+'pickup_cleaned.csv')


# Aggregating the data to get the number of trips to each end station
dropoff_counts = (
    dropoff.groupby(['DOLocationID','month','day_dropoff','dropoff_period'], as_index=False)
    .size()
    .rename(columns={'size': 'trip_count'})
)
# Fill NaN values in trip_count with 0 (if necessary)
dropoff_counts['trip_count'] = dropoff_counts['trip_count'].fillna(0)

dropoff_counts = dropoff_counts[dropoff_counts['trip_count'] > 0]

dropoff_counts.to_csv(path_or_buf=path+'dropoff_cleaned.csv')


# Grouping and aggregating pickup and dropoff data
pickup_dropoff_stats = (
    lyft.groupby(
        ['PULocationID', 'DOLocationID', 'month', 'day_pickup', 'pickup_period'],
        as_index=False,
        observed=True  # Explicitly set observed=True to handle categorical data properly
    )
    .agg(
        trip_count=('PULocationID', 'size'),        # Count trips
        avg_trip_distance=('trip_distance', 'mean'),  # Average distance
        avg_trip_duration=('trip_duration', 'mean')   # Average duration
    )
    .reset_index(drop=True)  # Reset index to avoid mismatches
)


# Drop rows with NaN values in any column
pickup_dropoff_stats = pickup_dropoff_stats.dropna()
pickup_dropoff_stats = pickup_dropoff_stats[pickup_dropoff_stats['trip_count'] > 0]

print(pickup_dropoff_stats.head())

pickup_dropoff_stats.to_csv(path_or_buf=path+'pickup_dropoff_cleaned.csv')