import urllib3
import boto3
import re

s3 = boto3.client('s3')
s3_resource = boto3.resource('s3')
http_pool = urllib3.PoolManager()

# Config for JSON file
json_file_name = 'data.json'
json_url = 'https://datausa.io/api/data?drilldowns=Nation&measures=Population'

# Config for CSV files
csv_root_url = 'https://download.bls.gov/pub/time.series/pr'
csv_bucket = 'rearc-pr-time-series-data'
csv_file_name = 'pr.data.0.Current'
csv_base_url = 'https://download.bls.gov'
csv_uri = '/pub/time.series/pr/'
pattern = re.compile(r'HREF=\"(.*?)\"[^>]*')


def save_url_to_s3(url, bucket, key):
    response = http_pool.request('GET', url)
    if response.status == 200:
        s3.put_object(Body=response.data.decode('UTF-8'), Bucket=bucket, Key=key)


def source_csv():
    # print(csv_root_url)
    response = http_pool.urlopen('GET', csv_base_url + csv_uri)
    csv_html_page = (response.data.decode('utf-8'))

    csv_files = []
    s3_files = list(s3_resource.Bucket(csv_bucket).objects.all())

    for (csv_link) in re.findall(pattern, csv_html_page):
        if (csv_link.startswith(csv_uri)):
            csv_files.append(csv_link.split(csv_uri)[1])

    # print("Adding/deleting new/old files")
    for mutation_file in list(set(csv_files) - set(s3_files)):
        if mutation_file in csv_files:
            # print("Add new file")
            print(mutation_file)
            save_url_to_s3(csv_base_url + csv_uri + mutation_file, csv_bucket, mutation_file)
        else:
            # print("Deleting old file")
            print(mutation_file)
            s3_resource.Object(csv_bucket, mutation_file).delete()

    # print("Updating commons files")
    for common_file in list(set(s3_files) & set(csv_files)):
        # print("Update common file")
        print(common_file)
        save_url_to_s3(csv_base_url + csv_uri + common_file, csv_bucket, common_file)


def source_json(json_bucket):
    # print("saving json to s3")
    save_url_to_s3(json_url, json_bucket, json_file_name)


def lambda_handler(event, context):
    source_csv()
    source_json(event['bucket'])
    return {
        'statusCode': 200
    }
