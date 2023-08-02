import boto3
import pandas as pd
import json
import numpy as np
from pathlib import Path
import logging 
import sys 
import psycopg2
from psycopg2 import extras
import os

# imports from folders 
from config.config import host, queue_url
from security.security_postgres import user, password


postgres_params = {
    'host': host,
    'user': user,
    'password': password
}

log_path = Path.cwd() / 'logs'

if not log_path.exists():
    os.makedirs(log_path)

logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(filename=log_path / 'aws_sqs_queue.log', mode='a'),
        logging.StreamHandler(sys.stdout)
             ]
                   )

# json file to dataframe 
def json_to_df(data):
    data_dict = json.loads(data)
    df = pd.DataFrame(data_dict, index=[0])
    return df

# pull messages from aws sqs 
def pull_sqs(queue_url):
    sqs_client = boto3.client('sqs', endpoint_url='http://localhost:4566')
    response = sqs_client.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=10,
        VisibilityTimeout=120, 
        WaitTimeSeconds=0     
    )
    if 'Messages' in response:
        message = response['Messages'][0]
        return message['Body']
    else:
        return None

# masking ip column data 
def mask_ip(data):
    return '.'.join(data.split('.')[:-1]) + '.' + '*' * (len(data) - data.rfind('.') - 1) if '.' in data else data

# masking device_id column data 
def mask_id(data):
    return '-'.join(data.split('-')[:-1]) + '-' + '*' * (len(data) - data.rfind('-') - 1) if '-' in data else data


if __name__ == "__main__":

    queue_url = queue_url 
    message = pull_sqs(queue_url)
    if message:

        # json to dataframe
        df = json_to_df(message)
        
        df['ip'] = df['ip'].apply(mask_ip)
        df['device_id'] = df['device_id'].apply(mask_id)
        df['app_version'] = df['app_version'].apply(lambda ver: int(''.join(filter(str.isdigit, ver))))

        # renaming column names
        df = df.rename(columns={ 'ip' : 'masked_ip', 'device_id' : 'masked_device_id'})
       
        # universal replacement 
        replacement = ['', np.nan, 'nan', 'NaN', 'NaT', '#N/A', 'NAN', ' ', '  ', 'N/A','NA', 'nat', 'nan']
        records = [
                    {
                        column: row[column] if str(row[column]) not in replacement else None
                        for column in df.columns
                    }   for row in df.to_dict(orient='records')
                  ] 
        
        # temp staging table
        query_temp_tbl = """
                          CREATE TEMP TABLE stg (                                  
                                user_id varchar(128),
                                device_type varchar(32),
                                masked_ip varchar(256),
                                masked_device_id varchar(256),
                                locale varchar(32),
                                app_version integer        
                          )
                          ON COMMIT PRESERVE ROWS
                          ;"""
        # loading data to stg temp table 
        query_load = """
                            INSERT INTO stg 
                            VALUES (
                                    %(user_id)s,
                                    %(device_type)s,
                                    %(masked_ip)s, 
                                    %(masked_device_id)s, 
                                    %(locale)s,
                                    %(app_version)s
                                    )
                             ;"""
        # upserting data from stg temp table to the user_logins table 
        query_upsert = """
                            INSERT INTO 
                                user_logins
                            SELECT *
                                   , CURRENT_DATE
                            FROM stg
                            ;"""
        
        try:
            with psycopg2.connect(**postgres_params) as conn:
                with conn.cursor() as cur:
                    
                    cur.execute(query_temp_tbl)
                    logging.info('Created temporary table')
                    
                    extras.execute_batch(cur, query_load, records)
                    logging.info(f'Loaded {len(records)} records to temp table')
                    
                    cur.execute(query_upsert)
                    logging.info(f'Upserted {cur.rowcount} records into target table')

        except (psycopg2.DatabaseError, Exception) as e:
            logging.info(f'Could not load to target ---> {e}')
            raise

        finally:
            conn.close()

    else:
        logging.info("No messages available in the queue.")
        

