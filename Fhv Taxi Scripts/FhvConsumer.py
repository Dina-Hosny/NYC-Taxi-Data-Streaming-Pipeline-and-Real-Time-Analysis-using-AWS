import json
import base64
import boto3
import pandas as pd
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
    try:
        # Initialize a DynamoDB client
        dynamodb = boto3.client('dynamodb')
        table_name = 'FhvTable'
        max_id = get_last_id(dynamodb, table_name)
        print(max_id)
        
        # List to hold the items for batch write
        batch_items = []

        for record in event['Records']:
            data = record['kinesis']['data']
            decoded_data = base64.b64decode(data).decode('utf-8')  # Decode the data if it is base64 encoded

            # Assuming each record is a separate item, write it to DynamoDB directly

            # Parse the decoded_data as JSON
            json_data = json.loads(decoded_data)

            # Convert the JSON data to a DataFrame
            fhv_df = pd.DataFrame.from_records([json_data])

            # Perform the provided transformations
            fhv_df['pickup_datetime'] = pd.to_datetime(fhv_df['pickup_datetime'], format='%Y-%m-%d %H:%M:%S')
            fhv_df['dropOff_datetime'] = pd.to_datetime(fhv_df['dropOff_datetime'], format='%Y-%m-%d %H:%M:%S')
            fhv_df['Trip_Duration'] = round((fhv_df['dropOff_datetime'] - fhv_df['pickup_datetime']).dt.total_seconds() / 60, 2)
            fhv_df['date'] = fhv_df['pickup_datetime'].dt.date.astype(str)
            fhv_df['SR_Flag'] = fhv_df['SR_Flag'].replace({1: 'Shared', 0: 'Non-Shared'})

            columns_to_drop = ['pickup_datetime', 'dropOff_datetime', 'PUlocationID', 'DOlocationID', 'Affiliated_base_number']
            fhv_df.drop(columns=columns_to_drop, inplace=True)

            ingestion_date = datetime.now().strftime('%d/%m/%Y')
            fhv_df['Ingestion_Date'] = ingestion_date

            fhv_df.reset_index(drop=True, inplace=True)
            new_order = ['ID', 'dispatching_base_num', 'date', 'SR_Flag', 'Trip_Duration', 'Ingestion_Date']
            fhv_df = fhv_df.reindex(columns=new_order)

            # Generate a new ID for each row
            fhv_df['ID'] = range(max_id + 1, max_id + 1 + len(fhv_df))
            max_id += len(fhv_df)

            # Convert DataFrame rows to dictionaries and append to batch_items
            for row in fhv_df.itertuples(index=False):
                item = {
                    'PutRequest': {
                        'Item': {
                            'ID': {'N': str(row.ID)},
                            'dispatching_base_num': {'S': row.dispatching_base_num},
                            'date': {'S': str(row.date)},
                            'SR_Flag': {'S': row.SR_Flag},
                            'Trip_Duration': {'N': str(row.Trip_Duration)},
                            'Ingestion_Date': {'S': row.Ingestion_Date}
                        }
                    }
                }
                batch_items.append(item)

        # Batch write the items to DynamoDB
        while batch_items:
            response = dynamodb.batch_write_item(
                RequestItems={
                    table_name: batch_items[:25]  # Write up to 25 items per batch
                }
            )
            unprocessed_items = response.get('UnprocessedItems', {})
            batch_items = unprocessed_items.get(table_name, [])

        return {
            'statusCode': 200,
            'body': 'Data successfully written to DynamoDB.'
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': str(e)
        }
