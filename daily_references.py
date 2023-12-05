# Resampling data to calculate the total duration for which sup_fan_mode is 1 each day
# Assuming each row represents a 5-minute interval
duration_per_day = raw_data[raw_data['sup_fan_mode'] == 1].resample('D', on='timestamp').sum() * 5  # in minutes

# Identify the days where this duration is at least 30 minutes
days_with_30min_sup_fan = duration_per_day[duration_per_day['sup_fan_mode'] >= 30].index

# Filter the original data for those days
filtered_data = raw_data[raw_data['timestamp'].dt.floor('D').isin(days_with_30min_sup_fan)]

# ==============================================================================================
# ===============================================================================================



# Next time step value calculation

# Group by date and shift the 'mix_air_temp' column to create 'next_mix_air_temp'
raw_data['next_ret_air_temp'] = raw_data.groupby('date')['ret_air_temp'].shift(-1)

# Drop rows where 'next_mix_air_temp' is NaN
raw_data.dropna(subset=['next_ret_air_temp'], inplace=True)
raw_data['delta_T'] = raw_data['ret_air_temp'] - raw_data['next_ret_air_temp']

# ==============================================================================================
# ===============================================================================================


# Reading from db and Pivoting
conn = pymysql.connect(host=db_host, port=db_port, user=db_user, passwd=db_passwd, database=db_name, cursorclass=pymysql.cursors.DictCursor)
cursor = conn.cursor()
sql = f"SELECT * FROM zigbee_object_value WHERE timestamp BETWEEN '{start_date}' AND '{end_date}'"
cursor.execute(sql)
result = cursor.fetchall()
raw_data = pd.DataFrame(result)

cursor.close()

raw_data = raw_data[raw_data['objectName'].str.contains('ArtHall_1_zigbee_temphumi')]
raw_data = raw_data.pivot(index='timestamp', columns='objectName', values='value').reset_index()
