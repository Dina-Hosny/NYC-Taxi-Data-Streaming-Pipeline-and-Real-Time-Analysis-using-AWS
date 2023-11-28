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
        table_name = 'HvfhvTable'
        max_id = get_last_id(dynamodb, table_name)

        # List to hold the items for batch write
        batch_items = []
        print('2')
        for record in event['Records']:
            data = record['kinesis']['data']
            decoded_data = base64.b64decode(data).decode('utf-8')  # Decode the data if it is base64 encoded
            print('3')
            # Assuming each record is a separate item, write it to DynamoDB directly

            # Parse the decoded_data as JSON
            json_data = json.loads(decoded_data)

            # Convert the JSON data to a DataFrame
            fhvhv = pd.DataFrame.from_records([json_data])


            # Perform the provided transformations
            fhvhv['pickup_datetime'] = pd.to_datetime(fhvhv['pickup_datetime'], format='%Y-%m-%d %H:%M:%S')
            fhvhv['dropoff_datetime'] = pd.to_datetime(fhvhv['dropoff_datetime'], format='%Y-%m-%d %H:%M:%S')
            
            
            fhvhv['Trip_Duration'] = round((fhvhv['dropoff_datetime'] - fhvhv['pickup_datetime']).dt.total_seconds() / 60, 2)
            fhvhv['date'] = fhvhv['pickup_datetime'].dt.date.astype(str)
            mapping = {'HV0002':'Juno' , 'HV0003': 'Uber' , 'HV0004' : 'Via' , 'HV0005': 'Lyft' }
            fhvhv['hvfhs_license_num'] = fhvhv['hvfhs_license_num'].map(mapping)
            fhvhv['hvfhs_license_num'] = fhvhv['hvfhs_license_num'].astype('string')
            
            fhvhv['trip_total_amount'] = fhvhv['base_passenger_fare']+fhvhv['tolls']+fhvhv['bcf']+fhvhv['sales_tax']+fhvhv['congestion_surcharge']+fhvhv['airport_fee']+fhvhv['tips']+fhvhv['driver_pay']
            fhvhv['trip_total_amount'] = fhvhv['trip_total_amount'].astype(float).round(2)

            mapping = {'N': 'Not shared' , 'Y':'Shared'}
            fhvhv['shared_match_flag'] = fhvhv['shared_match_flag'].map(mapping)
            fhvhv['shared_match_flag'] = fhvhv['shared_match_flag'].astype('string')
            dropColumns = ['on_scene_datetime','originating_base_num','access_a_ride_flag','pickup_datetime','dropoff_datetime','PULocationID','DOLocationID','wav_request_flag','shared_request_flag','base_passenger_fare','tolls','bcf','sales_tax','congestion_surcharge','airport_fee','driver_pay']
            fhvhv.drop(columns=dropColumns, axis=1, inplace=True)

            ingestion_date = datetime.now().strftime('%d/%m/%Y')
            fhvhv['Ingestion_Date'] = ingestion_date
            max_id = max_id + 1
            fhvhv['ID'] = max_id 
            fhvhv.reset_index(drop=True, inplace=True)
            new_order = ['ID', 'hvfhs_license_num', 'date', 'dispatching_base_num', 'Trip_Duration','trip_miles','trip_time','tips','trip_total_amount','shared_match_flag', 'Ingestion_Date']
            fhvhv = fhvhv.reindex(columns=new_order)
            # print('data before : ' , fhvhv)
            print(fhvhv.columns)

            for row in fhvhv.itertuples(index=False):
                item = {
                    'PutRequest': {
                        'Item': {
                            'ID': {'N': str(row.ID)},
                            'hvfhs_license_num': {'S': row.hvfhs_license_num},
                            'date': {'S': str(row.date)},
                            'dispatching_base_num': {'S': row.dispatching_base_num},
                            'Trip_Duration': {'N': str(row.Trip_Duration)},
                            'trip_miles': {'N': str(row.trip_miles)},
                            'trip_time': {'N': str(row.trip_time)},
                            'tips': {'N': str(row.tips)},
                            'trip_total_amount': {'N': str(row.trip_total_amount)},
                            'shared_match_flag': {'S': row.shared_match_flag},
                            'Ingestion_Date': {'S': row.Ingestion_Date}
                        }
                    }
                }
                batch_items.append(item)
        print('7')
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
