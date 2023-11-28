# NYC Taxi Data Streaming Pipeline and Real-Time Analysis using AWS

The project combines the capabilities of various AWS services to to implement a robust and scalable data pipeline for processing taxi-related data in real-time and generating analytical insights for informed decision-making.


## Project Details

A data pipeline that gets the data from NYC Taxi, simulates the “Real-Time” streaming, collects it into a queueing service, and then processes the data to build dashboards that help the business gain insights about how everything is working in the network itself and the behavior of the customers over time, in real-time and daily aggregated using AWS services.

## Project Layers:

![Pipeline](https://github.com/Dina-Hosny/NYC-Taxi-Data-Streaming-Pipeline-and-Real-Time-Analysis-using-AWS/assets/46838441/ec862345-672b-4afc-b779-631263444c70)


#### 1- Data Ingestion:
Source files are uploaded to Amazon S3, a highly scalable object storage service, to simulate real-time data.

#### 2- Serverless Processing:
AWS Lambda is employed as a serverless computing service. The Lambda application reads source files, randomly selects a variable number of records (ranging from 1 to 100), and processes them.

#### 3- Data Checkpoint:
Selected records are saved in Amazon DynamoDB, serving as a checkpoint to prevent duplicate data transmission in the future.

Additionally, the selected records are subtracted from the DynamoDB table and saved back to S3.

#### 4- Stream Processing:
-  **Producer:**
    Once the records are saved in S3, a Lambda producer is triggered upon file arrival.
- **Data Ingestion:**
    The arrival files are sent to an Amazon Kinesis stream, acting as a data ingestion layer.
- **Comsumer:**
    A Lambda consumer process retrieves the data from Kinesis and stores it in DynamoDB tables.

#### 5- Data Storage:
DynamoDB tables serve as both a staging area and a NoSQL database for real-time streaming.

#### 6- Streaming Modes:
The pipeline supports two main streams:

- *1- Real-Time Streaming*
- *2- Analytical Streaming*

#### Real-Time Streaming:
Amazon Athena, an interactive query service, is used to fetch real-time data from the DynamoDB table. The retrieved data is backed up on S3.

Power BI, a business analytics tool, automatically refreshes every 30 seconds using the data within Athena, enabling the creation of a real-time dashboard for monitoring purposes.

#### Analytical Streaming:
The analytical dashboard focuses on historical data.
At the end of each day, a Lambda function is triggered to extract data specific to that day, which is then stored in S3. Another Lambda function retrieves this data from S3 and writes it into an Amazon Redshift table, a powerful data warehousing solution. QuickSight, a cloud-based business intelligence tool, copies the data from Redshift and creates an analytical dashboard, providing insights into historical patterns and trends.

## Tools and Technologies:

All project pipelines are built using various AWS services.

* Amazon S3 
* AWS Lambda
* Amazon DynamoDB
* Amazon Kinesis
* Amazon Athena
* Amazon Redshift
* Amazon QuickSight
* Power BI
