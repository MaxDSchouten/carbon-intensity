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
    date_24h_behind = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    date_24h_ahead = (datetime.today() + timedelta(days=1)).strftime('%Y-%m-%d')
    date_48h_ahead = (datetime.today() + timedelta(days=2)).strftime('%Y-%m-%d')
    date_72h_ahead = (datetime.today() + timedelta(days=3)).strftime('%Y-%m-%d')
    dates = [date_24h_behind, current_date, date_24h_ahead, date_48h_ahead, date_72h_ahead]
    year = datetime.today().strftime('%Y')

    return dates, year


@task(description="Pulls the existing csv or creates a new one if it does not exist.")
def get_or_create_csv_on_s3(bucket, year):
    try:
        df = pd.read_csv(f's3://{bucket}/carbon_intensity/national/carbon_intensity_{year}.csv')
        
    except FileNotFoundError:
        df = pd.DataFrame(columns = ['time_from', 'time_to', 'forecast_intensity', 'actual_intensity',
                                     'index_intensity', 'date_recorded', 'biomass', 'coal', 'imports', 'gas',
                                     'nuclear', 'other', 'hydro', 'solar', 'wind'])
        

    return df


@task(description="Parses the data from the request and creates a row from it.")
def get_carbon_intensity(dates):
    carbon_intensity_df = pd.DataFrame(columns=['time_from', 'time_to', 'forecast_intensity', 'actual_intensity', 'index_intensity', 'date_recorded'])
    
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
                carbon_intensity_df.loc[len(carbon_intensity_df)] = row
            except IndexError:
                break
    
    carbon_intensity_df = carbon_intensity_df.set_index('time_from')
    return carbon_intensity_df


@task
def get_fuel_mix(dates):
    main_fuel_df = pd.DataFrame(columns=['from', 'to', 'biomass', 'coal', 'imports', 'gas', 'nuclear', 'other', 'hydro', 'solar', 'wind'])
    for date in dates:
        try:
            request = requests.get(f'https://api.carbonintensity.org.uk/generation/{date}/pt24h', params = {}, headers = headers)
            times = request.json()['data']
            df = pd.DataFrame(times)
            times_df = df[['from', 'to']]

            fuels_df = pd.DataFrame(columns=['biomass', 'coal', 'imports', 'gas', 'nuclear', 'other', 'hydro', 'solar', 'wind'])
            for period in range(len(request.json()['data'])):
                df = pd.DataFrame(request.json()['data'][period]['generationmix'])
                df = df.T
                df.columns = df.iloc[0]
                df = df.drop(df.index[0])
                fuels_df = pd.concat([fuels_df, df])

            
            fuels_df = fuels_df.reset_index(drop=True)

            fuel_mix_df = pd.concat([times_df, fuels_df], axis=1)
            

            main_fuel_df = pd.concat([main_fuel_df, fuel_mix_df], axis=0)
            
        except IndexError:

            break
            
    main_fuel_df = main_fuel_df.rename(columns={'from': 'time_from', 'to': 'time_to'})
    
    main_fuel_df = main_fuel_df.drop(['time_to'], axis=1)

    main_fuel_df = main_fuel_df.drop_duplicates(subset=['time_from'])
    
    main_fuel_df = main_fuel_df.set_index('time_from')

    return main_fuel_df


@task
def join_carbon_intensity_and_fuel_mix(carbon_intensity_df, fuel_mix_df):
    new_data_df = pd.concat([carbon_intensity_df, fuel_mix_df], axis=1, join='inner')

    return new_data_df


@task(description="Appends the row to the existing dataframe pulled from s3.")
def append_new_data(df_1, df_2):
    df = pd.concat([df_1.reset_index(), df_2.reset_index()]).drop_duplicates('time_from', keep='last')
    df = df.set_index('time_from')
    df = df.drop('index', axis=1)

    return df


@task(description="Uploads the updated dataframe to s3.")
def upload_csv_to_s3(df, bucket, year):
    df.to_csv(f's3://{bucket}/carbon_intensity/national/carbon_intensity_{year}.csv')


@task
def clean_date(bucket, year):
    df = pd.read_csv(f's3://{bucket}/carbon_intensity/national/carbon_intensity_{year}.csv')
    df[['time_to', 'time_from']] = df[['time_to', 'time_from']].apply(pd.to_datetime, format='%Y-%m-%dT%H:%MZ')
    df[['date_recorded']] = (df[['date_recorded']]).apply(pd.to_datetime, format='%Y-%m-%d')

    return df


@task
def upload_data_to_postgres(df):
    engine = create_engine("postgresql+psycopg://postgres:password@localhost:5432/carbon_intensity")
    df.to_sql('carbon_intensity', engine, if_exists='replace')


@flow
def carbon_intensity_pipeline(log_prints=True):
    dates, year = get_dates()

    old_data_df = get_or_create_csv_on_s3(bucket, year)

    carbon_intensity_df = get_carbon_intensity(dates)

    fuel_mix_df = get_fuel_mix(dates)

    new_data_df = join_carbon_intensity_and_fuel_mix(carbon_intensity_df, fuel_mix_df)

    df = append_new_data(old_data_df, new_data_df)

    upload_csv_to_s3(df, bucket, year)

    df = clean_date(bucket, year)

    upload_data_to_postgres(df)


if __name__=="__main__":
    carbon_intensity_pipeline.serve(
        name="carbon-intensity",
        cron="1/30 * * * *",
        description="Extract carbon intensity data."
    )