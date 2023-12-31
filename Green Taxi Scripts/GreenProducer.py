import pandas as pd
import io
import boto3
import random

def lambda_handler(event, context):
    # Set up the AWS clients
    s3_client = boto3.client('s3')
    kinesis_client = boto3.client('kinesis')

    # Specify the S3 bucket and object key
    s3_bucket = 'greentarget'
    s3_object_key = 'greenbatchs.parquet'

    # Specify the Kinesis Data Stream name
    kinesis_stream_name = 'green-streams'


    # Read the Parquet file from S3
    try:
        response = s3_client.get_object(Bucket=s3_bucket, Key=s3_object_key)
        parquet_file = response['Body'].read()
        dataframe = pd.read_parquet(io.BytesIO(parquet_file))

        
    except Exception as e:
        raise Exception(f"Failed to read data from S3: {e}")

    # Check if there are any records
    if dataframe.empty:
        raise Exception("No records found in the Parquet file")


    # Send the records to Kinesis Data Stream
    for _, record in dataframe.iterrows():
        print("Record:", record)
        kinesis_client.put_record(
            StreamName=kinesis_stream_name,
            Data=record.to_json().encode(),
            PartitionKey=str(1)
        )


# Call the lambda_handler function to trigger it manually
lambda_handler(None, None)
