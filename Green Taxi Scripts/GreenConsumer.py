import json
import base64
import boto3
import pandas as pd
import numpy as np
from datetime import datetime

def get_last_id(dynamodb, table_name):
    response = dynamodb.scan(
        TableName=table_name,
        ProjectionExpression='ID'
    )

    items = response['Items']
    max_id = 0

    while 'LastEvaluatedKey' in response:
        response = dynamodb.scan(
            TableName=table_name,
            ProjectionExpression='ID',
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        items.extend(response['Items'])

    for item in items:
        id_value = int(item['ID']['N'])
        if id_value > max_id:
            max_id = id_value

    return max_id

def lambda_handler(event, context):
    # Initialize a DynamoDB client
    dynamodb = boto3.client('dynamodb')
    table_name = 'GreenTables'
    max_id = get_last_id(dynamodb, table_name)

    # List to hold the items for batch write
    batch_items = []

    for record in event['Records']:
        data = record['kinesis']['data']
        decoded_data = base64.b64decode(data).decode('utf-8')  # Decode the data if it is base64 encoded

        # Assuming each record is a separate item, write it to DynamoDB directly

        # Parse the decoded_data as JSON
        json_data = json.loads(decoded_data)

        # Convert the JSON data to a DataFrame
        green_df = pd.DataFrame.from_records([json_data])

        # Perform the provided transformations
        print("Columns bewfore transformation:", green_df.columns)

        # print('Date before datetime conversion:', green_df['lpep_pickup_datetime'])
        # Convert datetime columns to datetime format
        green_df['lpep_pickup_datetime'] = pd.to_datetime(green_df['lpep_pickup_datetime'], format='%Y-%m-%d %H:%M:%S')
        green_df['lpep_dropoff_datetime'] = pd.to_datetime(green_df['lpep_dropoff_datetime'], format='%Y-%m-%d %H:%M:%S')

        # print('Date after datetime conversion:', green_df['lpep_pickup_datetime'])
        # Calculate Trip_Duration in minutes
        green_df['Trip_Duration'] = round((green_df['lpep_dropoff_datetime'] - green_df['lpep_pickup_datetime']).dt.total_seconds() / 60, 2)

        # Perform other transformations
        green_df['Vendor'] = green_df['VendorID'].map({1: 'Creative Mobile', 2: 'VeriFone Inc' , 0: 'Undefined'})
        green_df = green_df.replace({np.nan: ''})
        rate_mapping = {1: 'Standard rate', 2: 'JFK', 3: 'Newark', 4: 'Nassau or Westchester', 5: 'Negotiated fare', 6: 'Group ride', 0: 'Undefined'}
        green_df['RateCode'] = green_df['RatecodeID'].map(rate_mapping)
        payment_mapping = {1: 'Credit card', 2: 'Cash', 3: 'No Charge', 4: 'Dispute', 5: 'Unknown', 6: 'Voided trip' , 0: 'Undefined'}
        green_df['Payment'] = green_df['payment_type'].map(payment_mapping)
        trip_mapping = {1: 'Street hail', 2: 'Dispatch'}
        green_df['type_of_trip'] = green_df['trip_type'].map(trip_mapping)
        green_df['date'] = green_df['lpep_pickup_datetime'].dt.date
        print("1", green_df['date'])
        green_df['date'] = green_df['date'].astype(str)
        print("2", green_df['date'])
        columns_to_drop = ['lpep_pickup_datetime', 'VendorID', 'lpep_dropoff_datetime', 'store_and_fwd_flag', 'RatecodeID', 'PULocationID', 'DOLocationID', 'ehail_fee', 'payment_type', 'trip_type']
        green_df.drop(columns=columns_to_drop, inplace=True)
        green_df.reset_index(drop=True, inplace=True)
        ingestion_date = datetime.now().strftime('%d/%m/%Y')
        green_df['Ingestion_Date'] = ingestion_date

        max_id = max_id + 1

        green_df['ID'] = max_id
        new_order = ['ID', 'Vendor', 'date', 'RateCode', 'Payment', 'type_of_trip', 'Trip_Duration', 'passenger_count', 'trip_distance', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'Ingestion_Date']
        green_df = green_df.reindex(columns=new_order)

        # Replace nan values with empty string
        green_df = green_df.replace({np.nan: ''})

        # Extract the required attributes from the DataFrame
        transformed_json = green_df.iloc[0].to_dict()
        # Extract the values for DynamoDB attribute types
        partition_key = str(transformed_json["ID"])  # Convert to string
        # Construct the Item dictionary with specific columns
        item = {
            'ID': {'N': str(partition_key)},
            'Vendor': {'S': transformed_json['Vendor']},
            'date': {'S': transformed_json['date']},
            'RateCode': {'S': transformed_json['RateCode']},
            'Payment': {'S': transformed_json['Payment']},
            'type_of_trip': {'S': transformed_json['type_of_trip']},
            'Trip_Duration': {'N': str(transformed_json['Trip_Duration'])},
            'passenger_count': {'N': str(transformed_json['passenger_count'])},
            'trip_distance': {'N': str(transformed_json['trip_distance'])},
            'fare_amount': {'N': str(transformed_json['fare_amount'])},
            'extra': {'N': str(transformed_json['extra'])},
            'mta_tax': {'N': str(transformed_json['mta_tax'])},
            'tip_amount': {'N': str(transformed_json['tip_amount'])},
            'tolls_amount': {'N': str(transformed_json['tolls_amount'])},
            'improvement_surcharge': {'N': str(transformed_json['improvement_surcharge'])},
            'total_amount': {'N': str(transformed_json['total_amount'])},
            'congestion_surcharge': {'N': str(transformed_json['congestion_surcharge'])},
            'Ingestion_Date': {'S': ingestion_date}
        }
        print("Date:", transformed_json['date'])

        # Add the item to the batch_items list
        batch_items.append({
            'PutRequest': {
                'Item': item
            }
        })

    # Perform
    while batch_items:
        batch_write_items = batch_items[:25]  # Maximum batch size is 25 items
        batch_items = batch_items[25:]

        dynamodb.batch_write_item(
            RequestItems={
                table_name: batch_write_items
            }
        )

    return {
        'statusCode': 200,
        'body': 'Data written to DynamoDB in the same order.'
    }


