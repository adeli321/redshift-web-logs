#! /usr/bin/python3

import os
import sys
import psycopg2
import boto3
from redshift_connect import UseRedshift

aws_access_key = os.environ.get('AWS_ACCESS_KEY')
aws_secret_key = os.environ.get('AWS_SECRET_KEY')
db_name        = os.environ.get('DB_NAME')
redshift_user  = os.environ.get('REDSHIFT_USER')
redshift_pw    = os.environ.get('REDSHIFT_PW')
redshift_endpt = os.environ.get('REDSHIFT_ENDPT')
redshift_port  = os.environ.get('REDSHIFT_PORT')

if all(x in os.environ for x in ['AWS_ACCESS_KEY', 'AWS_SECRET_KEY', 
                                'DB_NAME', 'REDSHIFT_USER', 
                                'REDSHIFT_PW', 'REDSHIFT_ENDPT',
                                'REDSHIFT_PORT']):
    pass
else:
    print("""[INFO]:
            REQUIRED <===============================================
                AWS_ACCESS_KEY - AWS Access Key
                AWS_SECRET_KEY - AWS Secret Key
                DB_NAME - DB Name for Redshift
                REDSHIFT_USER - Username for Redshift
                REDSHIFT_PW - Password for Redshift
                REDSHIFT_ENDPT - Endpoint for Redshift
                REDSHIFT_PORT - Redshift Port
            ========================================================>
                    """)
    sys.exit(1)

redshift_db_config = {'dbname': db_name,
                    'host': redshift_endpt,
                    'port': redshift_port,
                    'user': redshift_user,
                    'password': redshift_pw}

def create_table():
    with UseRedshift(redshift_db_config) as cursor: 
        SQL_CREATE = rf"""CREATE TABLE IF NOT EXISTS public.s3_load (
                    date VARCHAR(20), 
                    time VARCHAR(20), 
                    server_ip VARCHAR(20), 
                    method VARCHAR(10), 
                    uri_stem VARCHAR(80), 
                    uri_query VARCHAR(1000), 
                    server_port VARCHAR(10), 
                    username VARCHAR(60), 
                    client_ip VARCHAR(20), 
                    client_browser VARCHAR(1000), 
                    client_cookie VARCHAR(1000), 
                    client_referrer VARCHAR(1000), 
                    status VARCHAR(10), 
                    substatus VARCHAR(10), 
                    win32_status VARCHAR(100), 
                    bytes_sent VARCHAR(100), 
                    bytes_received VARCHAR(100), 
                    duration VARCHAR(100), 
                    in_etl_1 BOOLEAN)
                    """ 
        try:
            print('-------- Executing CREATE TABLE --------')
            cursor.execute(SQL_CREATE)
            print('-------- SQL CREATE Complete --------')
        except Exception as err:
            print('Error executing SQL: ', err)

def insert_into_table():
    with UseRedshift(redshift_db_config) as cursor:
        s3_client = boto3.client('s3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key)

        ######## CODE TO ITERATE THROUGH ALL FILES IN S3 BUCKET ########
        # bucket_contents = s3_client.list_objects(Bucket='la-ticket-bucket-eu', Prefix='BI_logs/')
        # file_names = []
        # for i in bucket_contents['Contents']:
        #     file_names.append(i['Key'])
        # file_names.pop(0) # remove 'BI_logs' folder name from list, because it's not a specific file name
        # for log in file_names:

        file_object = s3_client.get_object(Bucket='la-ticket-bucket-eu', 
                                        Key='BI_logs/u_ex110407.log')
        file_contents = file_object['Body'].read().decode().split('\n')
        in_etl_1_flag = False
    
        split_logs = []
        for line in file_contents:
            split_logs.append(line.split(' '))

        for log in split_logs:
            if log[0].startswith('#'):
                pass
            else:
                if len(log) == 18:
                    print('-------- Executing INSERT Statement --------')
                    cursor.execute(rf"""INSERT INTO public.s3_load(
                                    date, time, server_ip, 
                                    method, uri_stem, uri_query, 
                                    server_port, username, client_ip, 
                                    client_browser, client_cookie, 
                                    client_referrer, status, 
                                    substatus, win32_status, 
                                    bytes_sent, bytes_received, 
                                    duration, in_etl_1) VALUES (%s, %s, %s, %s, %s, 
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                    %s, %s, %s, %s, %s)""", (log[0],
                                    log[1], log[2], log[3], log[4], log[5], 
                                    log[6], log[7], log[8], log[9], log[10], 
                                    log[11], log[12], log[13], log[14], log[15],
                                    log[16], log[17], in_etl_1_flag))
                    print('-------- Insert Statement Complete -------- ')
                elif len(log) == 14:
                    print('-------- Executing INSERT Statement --------')
                    cursor.execute(rf"""INSERT INTO public.s3_load(
                                    date, time, server_ip, 
                                    method, uri_stem, uri_query, 
                                    server_port, username, client_ip, 
                                    client_browser, status, 
                                    substatus, win32_status,  
                                    duration, in_etl_1) VALUES (%s, %s, %s, %s, %s, 
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s 
                                    )""", (log[0], log[1], log[2], log[3], log[4], log[5], 
                                    log[6], log[7], log[8], log[9], log[10], 
                                    log[11], log[12], log[13], in_etl_1_flag))
                    print('-------- Insert Statement Complete -------- ')
                else:
                    print('Unknown log length.')

if __name__ == '__main__':
    create_table()
    insert_into_table()



        # rf"""COPY public.s3_load FROM 's3://la-ticket-bucket-eu/u_ex091024.log'
        # IAM_ROLE 'arn:aws:iam::900056063831:role/RedshiftCopyUnload' DELIMITER '\n'"""
        # """COPY public.s3_load FROM 's3://la-ticket-bucket-eu/u_ex091024.log'
        # IAM_ROLE 'arn:aws:iam::900056063831:role/RedshiftCopyUnload' DELIMITER ' ';"""