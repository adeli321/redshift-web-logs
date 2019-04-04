#! usr/bin/python3

import os
import re
import sys
import uuid
import requests
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
        SQL_CREATE_DATE = rf"""CREATE TABLE IF NOT EXISTS public.dim_date (
                    id VARCHAR(50), 
                    date DATE, 
                    day INTEGER,
                    week INTEGER, 
                    month INTEGER, 
                    quarter INTEGER,
                    year INTEGER)"""
        SQL_CREATE_TIME = rf"""CREATE TABLE IF NOT EXISTS public.dim_time (
                    id VARCHAR(50), 
                    time TIMESTAMP, 
                    hour INTEGER, 
                    minute INTEGER, 
                    second INTEGER)"""
        SQL_CREATE_LOCATION = rf"""CREATE TABLE IF NOT EXISTS public.dim_location (
                    id VARCHAR(50), 
                    client_ip VARCHAR(200), 
                    postcode VARCHAR(10), 
                    city VARCHAR(300), 
                    region VARCHAR(200), 
                    country VARCHAR(200))"""
        SQL_CREATE_REQUEST = rf"""CREATE TABLE IF NOT EXISTS public.dim_request (
                    id VARCHAR(50), 
                    method VARCHAR(10), 
                    client_browser VARCHAR(1000), 
                    client_referrer VARCHAR(1000), 
                    status INTEGER, 
                    duration INTEGER)"""
        SQL_CREATE_FILE = rf"""CREATE TABLE IF NOT EXISTS public.dim_file (
                    id VARCHAR(50), 
                    uri_stem VARCHAR(80), 
                    bytes_sent INTEGER, 
                    file_type VARCHAR(10), 
                    is_crawler BOOLEAN)"""
        SQL_CREATE_VISIT = rf"""CREATE TABLE IF NOT EXISTS public.dim_visit (
                    id VARCHAR(50), 
                    client_cookie VARCHAR(1000))"""
        try:
            print('-------- Executing CREATE TABLE --------')
            cursor.execute(SQL_CREATE_DATE)
            cursor.execute(SQL_CREATE_TIME)
            cursor.execute(SQL_CREATE_LOCATION)
            cursor.execute(SQL_CREATE_REQUEST)
            cursor.execute(SQL_CREATE_FILE)
            cursor.execute(SQL_CREATE_VISIT)
            print('-------- SQL CREATE Complete --------')
        except Exception as err:
            print('Error executing SQL: ', err)

def insert_into_date():
    with UseRedshift(redshift_db_config) as cursor:
        cursor.execute(rf"""SELECT DISTINCT etl_1.date 
                            FROM public.etl_1 etl_1
                            LEFT JOIN public.dim_date dim_date 
                                ON dim_date.date = etl_1.date
                            WHERE etl_1.in_etl_2=False
                                AND dim_date.date IS NULL""")
        dates = cursor.fetchall()
        for i, date in zip(range(len(dates)), dates):
            date_str   = dates[i][0].isoformat()
            split_date = date_str.split('-')
            year       = split_date[0]
            month      = split_date[1]
            day        = split_date[2]
            if month in ['01','02','03']:
                qtr    = '1'
            elif month in ['04','05','06']:
                qtr    = '2'
            elif month in ['07','08','09']:
                qtr    = '3'
            else:
                qtr    = '4'
            date_id = str(uuid.uuid4())
            print('-------- Executing INSERT DATE Statement --------')
            cursor.execute(rf"""INSERT INTO public.dim_date(
                                id, date, day, month, quarter, year)
                                VALUES(%s, %s, %s, %s, %s, %s)""", 
                                (date_id, date, day, month, qtr, year))
            print('-------- Insert Statement Complete -------- ')

def insert_into_time():
    with UseRedshift(redshift_db_config) as cursor:
        cursor.execute(rf"""SELECT DISTINCT etl_1.time 
                            FROM public.etl_1 etl_1
                            LEFT JOIN public.dim_time dim_time 
                                ON dim_time.time = etl_1.time
                            WHERE etl_1.in_etl_2=False
                                AND dim_time.time IS NULL""")
        times = cursor.fetchall()
        for i, time in zip(range(len(times)), times):
            time_str   = times[i][0].isoformat()
            split_time = time_str.split('T') # returns ['2009-10-24', '01:40:40']
            hr_min_sec     = []
            for i in split_time:
                hr_min_sec = i.split(':') # returns ['01', '40', '40']
            hour    = hr_min_sec[0]
            minute  = hr_min_sec[1]
            second  = hr_min_sec[2]
            time_id = str(uuid.uuid4())
            print('-------- Executing INSERT TIME Statement --------')
            cursor.execute(rf"""INSERT INTO public.dim_time(
                                id, time, hour, minute, second)
                                VALUES(%s, %s, %s, %s, %s)""", 
                                (time_id, time, hour, minute, second))
            print('-------- Insert Statement Complete -------- ')

def insert_into_location():
    with UseRedshift(redshift_db_config) as cursor:
        cursor.execute(rf"""SELECT DISTINCT etl_1.client_ip
                            FROM public.etl_1 etl_1
                            LEFT JOIN public.dim_location dim_location
                                ON dim_location.client_ip = etl_1.client_ip
                            WHERE etl_1.in_etl_2=False
                                AND dim_location.client_ip IS NULL""")
        client_ips = cursor.fetchall()
        for ip in client_ips:
            results      = requests.get(f'https://ipinfo.io/{ip[0]}')
            results_dict = results.json()
            if 'postal' in results_dict.keys():
                postcode     = results_dict['postal']
            else:
                postcode = None
            if 'city' in results_dict.keys():
                city         = results_dict['city']
            else:
                city     = None
            if 'region' in results_dict.keys():
                region       = results_dict['region']
            else:
                region   = None
            country      = results_dict['country']
            location_id  = str(uuid.uuid4())
            print('-------- Executing INSERT LOCATION Statement --------')
            cursor.execute(rf"""INSERT INTO public.dim_location(
                                id, client_ip, postcode, city, region, country)
                                VALUES(%s, %s, %s, %s, %s, %s)""", 
                                (location_id, ip[0], postcode, city, region, country))
            print('-------- Insert Statement Complete -------- ')

def insert_into_request():
    with UseRedshift(redshift_db_config) as cursor:
        cursor.execute(rf"""SELECT DISTINCT etl_1.method, 
                                            etl_1.client_browser, 
                                            etl_1.client_referrer, 
                                            etl_1.status, 
                                            etl_1.duration
                            FROM public.etl_1 etl_1
                            LEFT JOIN public.dim_request dim_request
                                ON dim_request.method = etl_1.method
                                AND dim_request.client_browser = etl_1.client_browser
                                AND dim_request.client_referrer = etl_1.client_referrer
                                AND dim_request.status = etl_1.status
                                AND dim_request.duration = etl_1.duration
                            WHERE etl_1.in_etl_2=False
                                AND dim_request.method IS NULL
                                AND dim_request.client_browser IS NULL
                                AND dim_request.client_referrer IS NULL
                                AND dim_request.status IS NULL
                                AND dim_request.duration IS NULL""")
        request_results = cursor.fetchall()
        for i in request_results:
            method          = i[0]
            client_browser  = i[1]
            client_referrer = i[2]
            status          = i[3]
            duration        = i[4]
            request_id      = str(uuid.uuid4())
            print('-------- Executing INSERT REQUEST Statement --------')
            cursor.execute(rf"""INSERT INTO public.dim_request(
                                id, method, client_browser, client_referrer, status, duration)
                                VALUES(%s, %s, %s, %s, %s, %s)""", 
                                (request_id, method, client_browser, client_referrer, status, duration))
            print('-------- Insert Statement Complete -------- ')

def insert_into_file():
    with UseRedshift(redshift_db_config) as cursor:
        cursor.execute(rf"""SELECT DISTINCT etl_1.uri_stem, 
                                            etl_1.bytes_sent
                            FROM public.etl_1 etl_1
                            LEFT JOIN public.dim_file dim_file
                                ON dim_file.uri_stem = etl_1.uri_stem
                                AND dim_file.bytes_sent = etl_1.bytes_sent
                            WHERE etl_1.in_etl_2=False
                                AND dim_file.uri_stem IS NULL
                                AND dim_file.bytes_sent IS NULL""")

        file_contents = cursor.fetchall()
        for i in file_contents:
            uri_stem   = i[0]
            bytes_sent = i[1]
            file_ext  = re.search(r'\.[A-Za-z0-9]+$', i[0])
            if file_ext is not None:
                file_type = file_ext.group(0)
            else:
                file_type = file_ext
            if uri_stem == '/robots.txt':
                is_crawler = True
            else:
                is_crawler = False
            file_id    = str(uuid.uuid4())
            print('-------- Executing INSERT FILE Statement --------')
            cursor.execute(rf"""INSERT INTO public.dim_file(
                                id, uri_stem, bytes_sent, file_type, is_crawler)
                                VALUES(%s, %s, %s, %s, %s)""", 
                                (file_id, uri_stem, bytes_sent, file_type, is_crawler))
            print('-------- Insert Statement Complete -------- ')

def insert_into_visit():
    with UseRedshift(redshift_db_config) as cursor:
        cursor.execute(rf"""SELECT DISTINCT etl_1.client_cookie
                            FROM public.etl_1 etl_1
                            LEFT JOIN public.dim_visit dim_visit
                                ON dim_visit.client_cookie = etl_1.client_cookie
                            WHERE etl_1.in_etl_2=False
                                AND dim_visit.client_cookie IS NULL""")

        cookies = cursor.fetchall()
        for i in cookies:
            if i[0] == None:
                client_cookie = ''
            else:
                client_cookie  = i[0]
            visit_id       = str(uuid.uuid4())
            print('-------- Executing INSERT VISIT Statement --------')
            cursor.execute(rf"""INSERT INTO public.dim_visit(
                                id, client_cookie)
                                VALUES(%s, %s)""", 
                                (visit_id, client_cookie))
            print('-------- Insert Statement Complete -------- ')


def insert_ids_to_fact():
    with UseRedshift(redshift_db_config) as cursor:
        INSERT_DATE_ID = rf"""UPDATE public.etl_1
                            SET date_id = public.dim_date.id
                            FROM public.dim_date
                                WHERE public.etl_1.date = public.dim_date.date
                                AND public.etl_1.date_id IS NULL;"""
        INSERT_TIME_ID = rf"""UPDATE public.etl_1
                            SET time_id = public.dim_time.id
                            FROM public.dim_time
                                WHERE public.etl_1.time = public.dim_time.time
                                AND public.etl_1.time_id IS NULL;"""
        INSERT_LOCATION_ID = rf"""UPDATE public.etl_1
                                SET location_id = public.dim_location.id
                                FROM public.dim_location
                                    WHERE public.etl_1.client_ip = public.dim_location.client_ip
                                    AND public.etl_1.location_id IS NULL;"""
        INSERT_REQUEST_ID = rf"""UPDATE public.etl_1
                                SET request_id = public.dim_request.id
                                FROM public.dim_request
                                    WHERE public.etl_1.method        = public.dim_request.method
                                    AND public.etl_1.client_browser  = public.dim_request.client_browser
                                    AND (public.etl_1.client_referrer = public.dim_request.client_referrer 
                                        OR (public.etl_1.client_referrer IS NULL
                                            AND public.dim_request.client_referrer IS NULL))
                                    AND public.etl_1.status          = public.dim_request.status
                                    AND public.etl_1.duration        = public.dim_request.duration
                                    AND public.etl_1.request_id IS NULL;"""
                                    # fixed by accepting NULLs for client_referrer

        INSERT_FILE_ID = rf"""UPDATE public.etl_1
                            SET file_id = public.dim_file.id
                            FROM public.dim_file
                            WHERE public.etl_1.uri_stem = public.dim_file.uri_stem
                                AND (public.etl_1.bytes_sent = public.dim_file.bytes_sent
                                        OR (public.etl_1.bytes_sent IS NULL 
                                            AND public.dim_file.bytes_sent IS NULL))
                                AND public.etl_1.file_id IS NULL;"""
                                # fixed by accepting NULLs for bytes_sent
        INSERT_VISIT_ID = rf"""UPDATE public.etl_1
                            SET visit_id = public.dim_visit.id
                            FROM public.dim_visit
                            WHERE public.etl_1.client_cookie = public.dim_visit.client_cookie
                                AND public.etl_1.visit_id IS NULL;"""
        try:
            cursor.execute(INSERT_DATE_ID)
            cursor.execute(INSERT_TIME_ID)
            cursor.execute(INSERT_LOCATION_ID)
            cursor.execute(INSERT_REQUEST_ID)
            cursor.execute(INSERT_FILE_ID)
            cursor.execute(INSERT_VISIT_ID)
        except Exception as err:
            print('Error: ', err)

        cursor.execute(rf"""UPDATE public.etl_1 SET in_etl_2=True
                            WHERE in_etl_2=False""")


if __name__ == '__main__':
    create_table()
    insert_into_date()
    insert_into_time()
    insert_into_location()
    insert_into_request()
    insert_into_file()
    insert_into_visit()
    insert_ids_to_fact()
