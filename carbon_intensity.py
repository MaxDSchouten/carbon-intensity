from tasks import *
from prefect import task, flow


@task(description="Prepares s3 and sets the bucket, date and headers.")
def s3_preamble():
    s3 = boto3.client('s3')
    bucket = 'carbon-intensity-project'

    headers = {
        'Accept': 'application/json',
    }

    date = datetime.today().strftime('%Y-%m-%d')

    return bucket, headers, date


@task(description="Pulls the existing csv or creates a new one if it does not exist.")
def get_or_create_csv_on_s3(bucket, date):
    try:
        df = pd.read_csv(f's3://{bucket}/carbon_intensity/national/carbon_intensity_{date}.csv')
        
    except FileNotFoundError:
        df = pd.DataFrame(columns = ['time_from', 'time_to', 'forecast_intensity', 'actual_intensity', 'index_intensity'])

    return df


@task(description="Makes the initial request to the API.")
def make_request(date, headers):
    request = requests.get(f'https://api.carbonintensity.org.uk/intensity/date/{date}', params = {}, headers = headers)

    return request


@task(description="Parses the data from the request and creates a row from it.")
def get_data(request):
    time_from, time_to, forecast_intensity, actual_intensity, index_intensity = [], [], [], [], []
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
    
    row = zip(time_to, time_to, forecast_intensity, actual_intensity, index_intensity)

    return row


@task(description="Appends the row to the existing dataframe pulled from s3.")
def append_new_row(df, row):
    df_2 = pd.DataFrame(row, columns = ['time_from', 'time_to', 'forecast_intensity', 'actual_intensity', 'index_intensity'])
    df = pd.concat([df, df_2], ignore_index=True)

    return df


@task(description="Uploads the updated dataframe to s3.")
def upload_csv_to_s3(df, bucket, date):
    df.to_csv(f's3://{bucket}/carbon_intensity/national/carbon_intensity_{date}.csv', index=False)


@flow
def my_flow(log_prints=True):
    bucket, headers, date = s3_preamble()

    df_1 = get_or_create_csv_on_s3(bucket, date)

    request = make_request(date, headers)

    row = get_data(request)

    df_2 = append_new_row(df_1, row)

    upload_csv_to_s3(df_2, bucket, date)


if __name__=="__main__":
    my_flow.serve(
        name="carbon-intensity",
        cron="1/30 * * * *",
        description="Extract carbon intensity data."
    )