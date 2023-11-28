import requests
import boto3
import pandas as pd
from datetime import datetime, timedelta

s3 = boto3.client('s3')
bucket = 'carbon-intensity-project'

headers = {
    'Accept': 'application/json',
}

date = datetime.today().strftime('%Y-%m-%d')

try:
    df = pd.read_csv(f's3://{bucket}/carbon_intensity/national/carbon_intensity_{date}.csv')
    
except FileNotFoundError:
    df = pd.DataFrame(columns = ['time_from', 'time_to', 'forecast_intensity', 'actual_intensity', 'index_intensity'])

time_from, time_to, forecast_intensity, actual_intensity, index_intensity = [], [], [], [], []
request = requests.get(f'https://api.carbonintensity.org.uk/intensity/date/{date}', params = {}, headers = headers)
for period in range(0, 47):
    r = request.json()['data'][period]
    forecast = r['intensity']['forecast']
    actual = r['intensity']['actual']
    if forecast and pd.isna(actual) and not time_to:
        r = request.json()['data'][period-1]
        forecast = r['intensity']['forecast']
        actual = r['intensity']['actual']
        time_from.append(r['from'])
        time_to.append(r['to'])
        forecast_intensity.append(forecast)
        actual_intensity.append(actual)
        index_intensity.append(r['intensity']['index'])

df_2 = pd.DataFrame(zip(time_to, time_to, forecast_intensity, actual_intensity, index_intensity), 
columns = ['time_from', 'time_to', 'forecast_intensity', 'actual_intensity', 'index_intensity'])
df = pd.concat([df, df_2], ignore_index=True)

df.to_csv(f's3://{bucket}/carbon_intensity/national/carbon_intensity_{date}.csv', index=False)