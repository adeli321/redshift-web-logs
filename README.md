# redshift-web-logs
Repo for Business Intelligence project analysing web logs

The goal of this project was to analyse a collection of web logs. I did this by storing all of the files in an AWS S3 bucket. I then created an AWS Redshift cluster, and ran these three (s3_to_redshift, redshift_etl_1, & redshift_etl_2) scripts to migrate the data from S3 to Redshift, transform the data to the correct data types, derive additional columns from the data, and create dimension tables and a fact table. 
I then used Tableau to produce various graphs and visualisations from the fact and dimension tables created in Redshift.


