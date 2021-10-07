# AWSDataPipeline

Services Used:
- Lambda - Defines the process of execution
- S3 - Stores data from multiple sources (CSV & API)
- SQS - Gets populated each time data is loaded into S3
- CloudFormation - For orchestrating the data pipeline (data gets loaded to S3 via the API, which triggers  

This project consists of three different folders solving three parts of a data pipeline
# Part 1 & 2: AWS S3 & Sourcing Datasets (sources: CSV and API)
> - This folder contains the source code for Part 1 & Part 2.
> - The .py file (lambda function) loads data from two sources (viz, CSV and API) into S3 buckets
> - Link to data in S3 and source code:
>> - https://rearc-population-data.s3.amazonaws.com/data.json
>> - https://rearc-pr-time-series-data.s3.amazonaws.com/pr.data.0.Current

# Part 3: Data Analytics
> - This folder contains the source code for Part 3.
> - There is a AnalyticsNotebook.ipynb which contains the results within the notebook
> - Used Lambda Layer for importing pandas, boto3 packages

# Part 4: Infrastructure as Code & Data Pipeline with AWS CDK
> - This folder consists of the yaml file.
> - The yaml file defines consists the path on S3 where the code is saved, 
> 




