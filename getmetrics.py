import os
import boto3
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

access_key = os.getenv('AWS_ACCESS_KEY_ID')
secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
region = os.getenv('REGION')
resource_arn = os.getenv('RESOURCE_ARN')
queue_arn = os.getenv('QUEUE_ARN')

session = boto3.Session(
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
) 

connect = boto3.client('connect', region_name=region)

params = {
    "ResourceArn": resource_arn,  # Replace with your resource ARN
    "StartTime": datetime(2023, 9, 21),  # Replace with the start time
    "EndTime": datetime(2023, 10, 10),    # Replace with the end time
    "Interval": {
        "TimeZone": "UTC",               # Replace with your time zone
        "IntervalPeriod": "WEEK"         # Replace with desired interval period
    },
    "Filters": [
        {
            "FilterKey": "QUEUE",
            "FilterValues": [queue_arn]     # Replace with queue IDs or ARNs you want to filter on
        },
        # Add more filters as necessary
    ],
    "Groupings": ["QUEUE"],              # Replace with the grouping you want to apply
    "Metrics": [
        {
            "Name": "ABANDONMENT_RATE",
            # Add 'Threshold' and 'MetricFilters' if needed
        },
                {
            "Name": "AGENT_ANSWER_RATE",
            # Add 'Threshold' and 'MetricFilters' if needed
        },
        {
            "Name": "CONTACTS_ABANDONED",
            # Add 'Threshold' and 'MetricFilters' if needed
        },
        {
            "Name": "SERVICE_LEVEL",
            "Threshold": [
                {"Comparison": "LT", "ThresholdValue": 21}
                ],
        },
        {
            "Name": "CONTACTS_HANDLED",
            "MetricFilters": [
                {"MetricFilterKey": "INITIATION_METHOD",
                 "MetricFilterValues": ["INBOUND"]}
            ]
        },
        {
            "Name": "CONTACTS_HANDLED",
            "MetricFilters": [
                {"MetricFilterKey": "INITIATION_METHOD",
                 "MetricFilterValues": ["OUTBOUND"]}
            ]
        },
    ],
    # 'NextToken': 'string',             # Optional, for pagination
    # 'MaxResults': 123                  # Optional, to limit number of results
}

response = connect.get_metric_data_v2(**params)

# Assuming 'response' is the variable that holds the data you provided

# Extract the 'MetricResults' part of the response
metric_results = response.get('MetricResults', [])

df = pd.DataFrame(metric_results)

current_date = datetime.now().strftime("%Y-%m-%d")

# Save the DataFrame to an Excel file
excel_file = f"metrics_data_{current_date}.xlsx"
# excel_file = f"/tmp/metrics_data_{current_date}.xlsx"
df.to_excel(excel_file, index=False)

# Iterate over each result in 'MetricResults'
for result in metric_results:
    # Extract relevant data
    queue_id = result['Dimensions'].get('QUEUE')
    queue_arn = result['Dimensions'].get('QUEUE_ARN')
    start_time = result['MetricInterval']['StartTime']
    end_time = result['MetricInterval']['EndTime']
    
    print(f"Queue ID: {queue_id}, Queue ARN: {queue_arn}")
    print(f"Metric Interval: {start_time} to {end_time}")

    # Iterate over each collection in the result
    for collection in result['Collections']:
        metric_name = collection['Metric']['Name']
        metric_value = collection['Value']
        print(f"Metric Name: {metric_name}, Metric Value: {metric_value}")

    print("-" * 40)  # Just to separate different results
