## AWSDataPipeline

So, lets create a data pipeline using the Amazing Amazon Web Services!!! <br />
All we have to do, is to create an AWS account and work with a few different services! <br />
Step 1: Write a cool function to load data to S3 (but remember to implement code reusability) <br />
Step 2: Trigger a message to SQS queue as soon as the data is loaded to S3 <br />
Step 3: Recieve the message from SQS queue, fetch csv & json data from S3 and show off your data analytics skills using Pandas.

So, it looks something like this: <br /> <br />
![Or not](https://github.com/mishraapoorva/AWSDataPipeline/blob/master/img/rearc-dataflow.jpg)

### Services Used (- {& for what???}):
- Lambda - To write Functions (code) to perform the tasks like reading to and from S3, data analysis
- S3 - For Storing data from multiple sources (CSV & API)
- SQS - Gets populated each time data is loaded into S3
- CloudFormation - For creating and orchestrating the data pipeline  

## Also note,
This project consists of three folders solving three challenges of a data quest
### Part 1 & 2: AWS S3 & Sourcing Datasets (sources: CSV and API)
> - This folder contains the source code for Part 1 & Part 2 combined
> - The .py file (lambda function) loads data from two sources (viz, CSV and API) into S3 buckets
> - Link to data in S3 and source code:
>> - https://rearc-population-data-1633606565.s3.amazonaws.com/data.json
>> - https://rearc-pr-time-series-data.s3.amazonaws.com/pr.data.0.Current

### Part 3: Data Analytics
> - This folder contains the source code for Part 3
> - There is a AnalyticsNotebook.ipynb which contains the results within the notebook
> - Used Lambda Layer for importing pandas, boto3 packages

### Part 4: Infrastructure as Code & Data Pipeline with AWS CloudFormation
> - Create a neat, little stack using yaml template to define the resources using CloudFormation <br />
> - This folder consists of the cloudFormation.yaml file
> 





