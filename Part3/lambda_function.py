import json
import boto3
import pandas as pd
from io import StringIO

s3 = boto3.client('s3')

# Config for JSON file
json_bucket = 'rearc-population-data'
json_file_name = 'data.json'

# Config for CSV files
csv_bucket = 'rearc-pr-time-series-data'
csv_file_name = 'pr.data.0.Current'

def print_analytics_result():
    csv_data = StringIO(s3.get_object(Bucket= csv_bucket, Key= csv_file_name)['Body'].read().decode('utf-8'))
    time_series_csv_df = pd.read_csv(csv_data, sep='\t')
    time_series_csv_df.columns = time_series_csv_df.columns.str.strip()
    time_series_csv_df_grouped = time_series_csv_df.groupby(['series_id','year']).agg({'value': 'sum'}).reset_index()
    # print(time_series_csv_df_grouped.head(30)) 
    result = time_series_csv_df_grouped.groupby(['series_id'], as_index=False)['value'].max().rename(columns={'value': 'value'})
    time_series_report = time_series_csv_df_grouped.merge(result, on=['series_id', 'value'])
    print(time_series_report.head()) 
    
    population_json = json.loads(s3.get_object(Bucket= json_bucket, Key= json_file_name)['Body'].read().decode('utf-8'))
    temp_data =  population_json["data"]
    population_df = pd.DataFrame(temp_data)
    population_df = population_df.rename(columns={'Year': 'year'})
    population_df['year']=population_df['year'].astype(int)
    clubbed_df = time_series_csv_df.merge(population_df, on=['year'])
    clubbed_df.drop(['footnote_codes', 'ID Nation','Nation', 'ID Year', 'Slug Nation'], axis=1, inplace=True)
    print(clubbed_df.head())
    
def lambda_handler(event, context):
    
    print_analytics_result()
    return {
        'statusCode': 200
    }
