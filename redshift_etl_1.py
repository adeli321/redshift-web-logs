#! /usr/bin/python3

import os
import sys
import uuid
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
        SQL_CREATE = rf"""CREATE TABLE IF NOT EXISTS public.etl_1 (
                    id VARCHAR(50), 
                    date_id VARCHAR(50), 
                    time_id VARCHAR(50), 
                    location_id VARCHAR(50), 
                    request_id VARCHAR(50), 
                    file_id VARCHAR(50), 
                    visit_id VARCHAR(50),
                    date DATE, 
                    time TIMESTAMP, 
                    server_ip VARCHAR(20), 
                    method VARCHAR(10), 
                    uri_stem VARCHAR(80), 
                    uri_query VARCHAR(1000), 
                    server_port INT, 
                    username VARCHAR(60), 
                    client_ip VARCHAR(20), 
                    client_browser VARCHAR(1000), 
                    client_cookie VARCHAR(1000), 
                    client_referrer VARCHAR(1000), 
                    status INT, 
                    substatus INT, 
                    win32_status INT, 
                    bytes_sent INT, 
                    bytes_received INT, 
                    duration INT, 
                    in_etl_2 BOOLEAN)""" 

        try:
            print('-------- Executing CREATE TABLE --------')
            cursor.execute(SQL_CREATE)
            print('-------- SQL CREATE Complete --------')
        except Exception as err:
            print('Error executing SQL: ', err)

def insert_into_table():
    with UseRedshift(redshift_db_config) as cursor:

        # Select all if not already present in public.etl_1 table
        cursor.execute(rf"""SELECT * FROM public.s3_load WHERE in_etl_1 = False""")

        in_etl_2_flag = False

        logs = cursor.fetchall()
        for single_log in logs:
            if len(single_log) == 19:
                new_row = []
                new_row.append(single_log[0]) # append date
                new_timestamp = single_log[0:2]
                new_timestamp = " ".join(new_timestamp)
                new_row.append(new_timestamp) # append timestamp
                new_row.append(single_log[2]) # append server_ip
                new_row.append(single_log[3]) # append method
                new_row.append(single_log[4]) # append uri_stem
                new_row.append(single_log[5]) # append uri_query
                server_port = int(single_log[6])
                new_row.append(server_port) # append server_port
                new_row.append(single_log[7]) # append username
                new_row.append(single_log[8]) # append client_ip
                new_row.append(single_log[9]) # append client_browser
                if single_log[10] == None:
                    new_row.append('')
                else:
                    new_row.append(single_log[10]) # append client_cookie
                new_row.append(single_log[11]) # append client_referrer
                status = int(single_log[12])
                new_row.append(status) # append status
                sub_status = int(single_log[13])
                new_row.append(sub_status) # append sub_status
                win32_status = int(single_log[14])
                new_row.append(win32_status) # append win32_status
                if single_log[15] == None:
                    new_row.append(single_log[15]) # append bytes_sent
                else:
                    bytes_sent = int(single_log[15])
                    new_row.append(bytes_sent) # append bytes_sent
                if single_log[16] == None:
                    new_row.append(single_log[16])
                else:
                    bytes_received = int(single_log[16])
                    new_row.append(bytes_received) # append bytes_received
                duration = int(single_log[17])
                new_row.append(duration) # append duration
                log_id = str(uuid.uuid4())
                print('.')
                cursor.execute(rf"""INSERT INTO public.etl_1(
                                id, date, time, server_ip, 
                                method, uri_stem, uri_query, 
                                server_port, username, client_ip, 
                                client_browser, client_cookie, 
                                client_referrer, status, 
                                substatus, win32_status, 
                                bytes_sent, bytes_received, 
                                duration, in_etl_2) VALUES (%s, %s, %s, %s, %s, %s, 
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                %s, %s, %s, %s, %s)""", (log_id, new_row[0],
                                new_row[1], new_row[2], new_row[3], new_row[4], new_row[5], 
                                new_row[6], new_row[7], new_row[8], new_row[9], new_row[10], 
                                new_row[11], new_row[12], new_row[13], new_row[14], new_row[15],
                                new_row[16], new_row[17], in_etl_2_flag))
                print('.')
            elif len(single_log) == 15:
                new_row = []
                new_row.append(single_log[0]) # append date
                new_timestamp = single_log[0:2]
                new_timestamp = " ".join(new_timestamp)
                new_row.append(new_timestamp) # append timestamp
                new_row.append(single_log[2]) # append server_ip
                new_row.append(single_log[3]) # append method
                new_row.append(single_log[4]) # append uri_stem
                new_row.append(single_log[5]) # append uri_query
                server_port = int(single_log[6])
                new_row.append(server_port) # append server_port
                new_row.append(single_log[7]) # append username
                new_row.append(single_log[8]) # append client_ip
                new_row.append(single_log[9]) # append client_browser
                status = int(single_log[10])
                new_row.append(status) # append status
                sub_status = int(single_log[11])
                new_row.append(sub_status) # append sub_status
                win32_status = int(single_log[12])
                new_row.append(win32_status) # append win32_status
                duration = int(single_log[13])
                new_row.append(duration) # append duration
                new_row.append('') # to replace client_cookie NULL with empty string
                log_id = str(uuid.uuid4())
                print('.')
                cursor.execute(rf"""INSERT INTO public.etl_1(
                                id, date, time, server_ip, 
                                method, uri_stem, uri_query, 
                                server_port, username, client_ip, 
                                client_browser, status, 
                                substatus, win32_status,  
                                duration, client_cookie, in_etl_2) VALUES (%s, %s, %s, %s, %s, %s, 
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                                %s)""", (log_id, new_row[0],
                                new_row[1], new_row[2], new_row[3], new_row[4], new_row[5], 
                                new_row[6], new_row[7], new_row[8], new_row[9], new_row[10], 
                                new_row[11], new_row[12], new_row[13], new_row[14], in_etl_2_flag))
                print('.')
            else:
                print('not 18 columns long')

        cursor.execute(rf"""UPDATE public.s3_load SET in_etl_1=True
                            WHERE in_etl_1=False""")


if __name__ == '__main__':
    create_table()
    insert_into_table()

