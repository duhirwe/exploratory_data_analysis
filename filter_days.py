# Resampling data to calculate the total duration for which sup_fan_mode is 1 each day
# Assuming each row represents a 5-minute interval
duration_per_day = raw_data[raw_data['sup_fan_mode'] == 1].resample('D', on='timestamp').sum() * 5  # in minutes

# Identify the days where this duration is at least 30 minutes
days_with_30min_sup_fan = duration_per_day[duration_per_day['sup_fan_mode'] >= 30].index

# Filter the original data for those days
filtered_data = raw_data[raw_data['timestamp'].dt.floor('D').isin(days_with_30min_sup_fan)]