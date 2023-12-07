from prefect import task, flow
import requests
import boto3
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine

s3 = boto3.client('s3')
bucket = 'carbon-intensity-project'

headers = {
        'Accept': 'application/json',
    }


@task(description="Prepares s3 and sets the bucket, date and headers.")
def get_dates():
    current_date = (datetime.today()).strftime('%Y-%m-%d')
    date_24h_ahead = (datetime.today() + timedelta(days=1)).strftime('%Y-%m-%d')
    date_48h_ahead = (datetime.today() + timedelta(days=2)).strftime('%Y-%m-%d')
    dates = [current_date, date_24h_ahead, date_48h_ahead]
    year = datetime.today().strftime('%Y')

    return dates, year


@task(description="Pulls the existing csv or creates a new one if it does not exist.")
def get_or_create_csv_on_s3(bucket, year):
    try:
        df = pd.read_csv(f's3://{bucket}/carbon_intensity/national/carbon_intensity_{year}.csv')
        
    except FileNotFoundError:
        df = pd.DataFrame(columns = ['time_from', 'time_to', 'forecast_intensity', 'actual_intensity', 'index_intensity', 'date_recorded'])

    return df


@task(description="Parses the data from the request and creates a row from it.")
def get_data(dates):
    df = pd.DataFrame(columns=['time_from', 'time_to', 'forecast_intensity', 'actual_intensity', 'index_intensity', 'date_recorded'])
    for date in dates:
        try:
            request = requests.get(f'https://api.carbonintensity.org.uk/intensity/date/{date}', params = {}, headers = headers)
            
        except:
            print('Request failed.')
            
        time_from, time_to, forecast_intensity, actual_intensity, index_intensity, date_recorded = [], [], [], [], [], []
        for period in range(0, 47):
            try:
                r = request.json()['data'][period]
                forecast = r['intensity']['forecast']
                actual = r['intensity']['actual']
                r = request.json()['data'][period-1]
                forecast = r['intensity']['forecast']
                actual = r['intensity']['actual']
                time_from = r['from']
                time_to = r['to']
                forecast_intensity = forecast
                actual_intensity = actual
                index_intensity = r['intensity']['index']
                date_recorded = date
                row = [time_from, time_to, forecast_intensity, actual_intensity, index_intensity, date_recorded]
                df.loc[len(df)] = row
            except IndexError:
                
                break

    return df


@task(description="Appends the row to the existing dataframe pulled from s3.")
def append_new_data(df_1, df_2):
    df = pd.concat([df_1,df_2]).drop_duplicates('time_from')

    return df


@task(description="Uploads the updated dataframe to s3.")
def upload_csv_to_s3(df, bucket, year):
    df.to_csv(f's3://{bucket}/carbon_intensity/national/carbon_intensity_{year}.csv', index=False)


@task
def clean_date(bucket, year):
    df = pd.read_csv(f's3://{bucket}/carbon_intensity/national/carbon_intensity_{year}.csv')
    df[['time_to', 'time_from']] = df[['time_to', 'time_from']].apply(pd.to_datetime, format='%Y-%m-%dT%H:%MZ')
    df[['date_recorded']] = (df[['date_recorded']]).apply(pd.to_datetime, format='%Y-%m-%d')

    return df


@task
def upload_data_to_postgres(df):
    engine = create_engine("postgresql+psycopg://postgres:password@localhost:5432/carbon_intensity")
    df.to_sql('carbon_intensity', engine, if_exists='append', index=False)


@flow
def carbon_intensity_pipeline(log_prints=True):
    dates, year = get_dates()

    df_1 = get_or_create_csv_on_s3(bucket, year)

    df_2 = get_data(dates)

    df = append_new_data(df_1, df_2)

    upload_csv_to_s3(df, bucket, year)

    df = clean_date(bucket, year)

    upload_data_to_postgres(df)


if __name__=="__main__":
    carbon_intensity_pipeline.serve(
        name="carbon-intensity",
        cron="2/30 * * * *",
        description="Extract carbon intensity data."
    )