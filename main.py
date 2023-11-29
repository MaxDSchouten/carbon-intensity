from tasks import *

bucket, headers, date = s3_preamble()

df_1 = get_or_create_csv_on_s3(bucket, date)

request = make_request(date, headers)

row = get_data(request)

df_2 = append_new_row(df_1, row)

upload_csv_to_s3(df_2, bucket, date)

