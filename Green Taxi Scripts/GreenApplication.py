import boto3
import pandas as pd
import io
import uuid
import numpy as np
import random


def lambda_handler(event, context):
    # Set up the AWS clients
    s3_client = boto3.client('s3')
    dynamodb_resource = boto3.resource('dynamodb')

    # Specify the S3 bucket and object key
    s3_bucket = 'taxisourcefiles'
    s3_object_key = 'green_final.parquet'

    # Specify the DynamoDB table name
    dynamodb_table_name = 'check'
    # Generate a random range of records to select
    min_records = 1
    max_records = 100

    # Select a subset of records randomly
    num_records = random.randint(min_records, max_records)

    # Read the Parquet file from S3
    try:
        response = s3_client.get_object(Bucket=s3_bucket, Key=s3_object_key)
        parquet_file = response['Body'].read()

        # Convert the Parquet file to a Pandas DataFrame
        new_dataframe = pd.read_parquet(io.BytesIO(parquet_file))

    except Exception as e:
        raise Exception(f"Failed to read data from S3: {e}")

    # Check if there are any records in the new DataFrame
    if new_dataframe.empty:
        raise Exception("No records found in the Parquet file")

    # Convert NaN and None values to 0
    new_dataframe = new_dataframe.fillna(0)
    new_dataframe = new_dataframe.replace({None: 0})

    # Convert all columns to strings
    new_dataframe = new_dataframe.astype(str)

    # Get the existing data from the DynamoDB table
    existing_data = pd.DataFrame()
    try:
        dynamodb_table = dynamodb_resource.Table(dynamodb_table_name)
        response = dynamodb_table.scan()
        items = response['Items']
        while 'LastEvaluatedKey' in response:
            response = dynamodb_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])
        existing_data = pd.DataFrame(items)
        
    except Exception as e:
        raise Exception(f"Failed to read data from DynamoDB table: {e}")

    # Check if there are any records in the existing data
    if existing_data.empty:
        # Insert all records from the new DataFrame into the DynamoDB table
        new_data_records = new_dataframe.head(num_records).to_dict(orient='records')
        for record in new_data_records:
            record['ID'] = str(uuid.uuid4())
        with dynamodb_table.batch_writer() as batch:
            for record in new_data_records:
                batch.put_item(Item=record)
    else:
        # Compare the data in both DataFrames and get unique records
        existing_data = existing_data.drop('ID', axis=1)
        merged_data = pd.concat([existing_data, new_dataframe])
        unique_data = merged_data.drop_duplicates(keep=False)

        columns_to_convert = {
            'VendorID': 'int64',
            'RatecodeID': 'float64',
            'passenger_count': 'float64',
            'trip_distance': 'float64',
            'fare_amount': 'float64',
            'extra': 'float64',
            'mta_tax': 'float64',
            'tip_amount': 'float64',
            'tolls_amount': 'float64',
            'improvement_surcharge': 'float64',
            'total_amount': 'float64',
            'payment_type': 'float64',
            'trip_type': 'float64',
            'congestion_surcharge': 'float64'
        }
        
        # Select the first 100 records from the unique DataFrame
        records_to_insert = unique_data.head(num_records)
        
        s3batch = records_to_insert.astype(columns_to_convert)
        # Write the DataFrame to S3 as a Parquet file
        s3_filename = 'greenbatchs.parquet'
        s3_bucket_name = 'greentarget'
        s3_key = f'{s3_filename}'

        # Save the DataFrame as Parquett
        with io.BytesIO() as buffer:
            s3batch.to_parquet(buffer, engine='pyarrow')
            buffer.seek(0)
            s3_client.upload_fileobj(buffer, s3_bucket_name, s3_key)

        # Convert the DataFrame records to a list of dictionaries
        records_to_insert = records_to_insert.to_dict(orient='records')
        print('Records sents are:',num_records)
        # Generate a unique ID for each item
        for record in records_to_insert:
            record['ID'] = str(uuid.uuid4())

        # Insert the records into the DynamoDB table
        with dynamodb_table.batch_writer() as batch:
            for record in records_to_insert:
                batch.put_item(Item=record)

