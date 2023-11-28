import json
import base64
import boto3
import pandas as pd
import numpy as np
from datetime import datetime


def get_last_yellow_id(dynamodb,table_name):

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
    table_name = 'YellowTables'
    
    max_id = get_last_yellow_id(dynamodb, table_name)
    
    batch_items = []

    
    for record in event['Records']:
        data = record['kinesis']['data']
        decoded_data = base64.b64decode(data).decode('utf-8')  # Decode the data if it is base64 encoded
    
        # Parse the decoded_data as JSON
        json_data = json.loads(decoded_data)
        yellow_df = pd.DataFrame.from_records([json_data])
        
        yellow_df['tpep_pickup_datetime'] = pd.to_datetime(yellow_df['tpep_pickup_datetime'], format='%Y-%m-%d %H:%M:%S')
        yellow_df['tpep_dropoff_datetime'] = pd.to_datetime(yellow_df['tpep_dropoff_datetime'], format='%Y-%m-%d %H:%M:%S')
        
        yellow_df['Vendor'] = yellow_df['VendorID'].map({1: 'Creative Mobile', 2: 'VeriFone Inc' , 0:'Undefined'})
        yellow_df['Trip_Duration'] = round((yellow_df['tpep_dropoff_datetime'] - yellow_df['tpep_pickup_datetime']).dt.total_seconds() / 60, 2)
        
        rate_mapping = {1: 'Standard rate', 2: 'JFK', 3: 'Newark', 4: 'Nassau or Westchester', 5: 'Negotiated fare', 6: 'Group ride' ,  0:'Undefined'}
        yellow_df['RateCode'] = yellow_df['RatecodeID'].map(rate_mapping)
        
        payment_mapping = {1: 'Credit card', 2: 'Cash', 3: 'No Charge', 4: 'Dispute', 5: 'Unknown', 6: 'Voided trip',  0:'Undefined'}
        yellow_df['Payment'] = yellow_df['payment_type'].map(payment_mapping)
        
        # yellow_df['date'] = yellow_df['tpep_pickup_datetime'].dt.date
        yellow_df['date'] = yellow_df['tpep_pickup_datetime'].dt.date.astype(str)  # Convert date to string
    
        columns_to_drop = ['tpep_pickup_datetime', 'VendorID', 'tpep_dropoff_datetime', 'store_and_fwd_flag','RatecodeID', 'PULocationID', 'DOLocationID', 'payment_type']
        yellow_df.drop(columns=columns_to_drop, inplace=True)
        
        max_id = max_id + 1
        yellow_df['ID'] = max_id
        ingestion_date = datetime.now().strftime('%d/%m/%Y')
        yellow_df['Ingestion_Date'] = ingestion_date
        yellow_df.reset_index(drop=True, inplace=True)
        new_order = ['ID', 'Vendor' , 'date' , 'RateCode', 'Payment', 'Trip_Duration', 'passenger_count', 'trip_distance', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'airport_fee' , 'Ingestion_Date']
        yellow_df = yellow_df.reindex(columns=new_order)
        yellow_df = yellow_df.replace({np.nan: ''})
    
        # Store yellow_df to DynamoDB table for yellow data
        
        transformed_json = yellow_df.iloc[0].to_dict()
        # Extract the values for DynamoDB attribute types
        partition_key = str(transformed_json["ID"])  # Convert to string
        # Construct the Item dictionary with specific columns
        item = {
            'ID': {'N': str(partition_key)},
            'Vendor': {'S': transformed_json['Vendor']},
            'date': {'S': transformed_json['date']},
            'RateCode': {'S': transformed_json['RateCode']},
            'Payment': {'S': transformed_json['Payment']},
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
            'airport_fee' : {'N': str(transformed_json['airport_fee'])},
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
