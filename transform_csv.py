import pandas as pd
from datetime import timedelta
import os

# Function to apply transformations to a single dataframe
def transform_data(df):
    # Converting 'Start Time' to datetime
    if 'Start Time' in df.columns:
        df['Start Time'] = pd.to_datetime(df['Start Time'], format='%I:%M:%S %p')

    def convert_to_timedelta(duration):
        # Split by ":" into a maximum of 3 parts (hours, minutes, seconds)
        parts = duration.split(":", maxsplit=2)
        
        if len(parts) == 2:  # minutes:seconds
            minutes, seconds = map(int, parts)
            return pd.Timedelta(minutes=minutes, seconds=seconds)
        elif len(parts) == 3:  # hours:minutes:seconds
            hours, minutes, seconds = map(int, parts)
            return pd.Timedelta(hours=hours, minutes=minutes, seconds=seconds)
        else:
            return pd.Timedelta(0)  # Handle unexpected format

    # Apply the conversion to the 'Duration' columns
    duration_columns = [
        'Duration  Total (min:sec)', 
        'DurationSpeed Zone 1 (min:sec)', 
        'DurationSpeed Zone 2 (min:sec)', 
        'DurationSpeed Zone 3 (min:sec)', 
        'DurationSpeed Zone 4 (min:sec)', 
        'DurationSpeed Zone 5 (min:sec)'
    ]
    
    for col in duration_columns:
        if col in df.columns:  # Check if column exists in the dataframe
            df[col] = df[col].apply(convert_to_timedelta)

    # Convert 'Start Time' to only time (hour, minute, second)
    if 'Start Time' in df.columns:
        df['Start Time'] = df['Start Time'].dt.time

    # Convert each 'Duration' from timedelta to minutes and seconds
    for col in duration_columns:
        if col in df.columns:  # Check if column exists in the dataframe
            df[col] = df[col].dt.total_seconds()  # Get the total seconds
            df[col] = df[col].apply(lambda x: f'{int(x // 60):02}:{int(x % 60):02}')  # Convert seconds to min:sec

    return df

# Directory where CSVs are downloaded
data_dir = "CSV"
all_data = pd.DataFrame()  # Empty DataFrame to hold all the merged data

# Read and transform each CSV dynamically from the 'data' folder
for file_name in os.listdir(data_dir):
    if file_name.endswith(".csv"):
        file_path = os.path.join(data_dir, file_name)
        df = pd.read_csv(file_path)  # Read the CSV file
        transformed_df = transform_data(df)  # Apply transformations
        all_data = pd.concat([all_data, transformed_df], ignore_index=True)  # Append the data

# Print the merged transformed data (optional)
print(all_data)

# Save the merged transformed data to a CSV file
all_data.to_csv("transformed_file.csv", index=False)

