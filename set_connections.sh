#!/bin/bash
#
# TO-DO: run the follwing command and observe the JSON output: 
# airflow connections get aws_credentials -o json 
# 
#[{"id": "1", 
# "conn_id": "aws_credentials",
# "conn_type": "aws", 
# "description": "", 
# "host": "", 
# "schema": "", 
# "login": "************", 
# "password": "***********************", 
# "port": null, 
# "is_encrypted": "True", 
# "is_extra_encrypted": "True", 
# "extra_dejson": {"region_name": "us-east-1"}, 
# "get_uri": "aws://****************************region_name=us-east-1"
#}]
#
# TO-DO: Update the following command with the URI and un-comment it:
#
airflow connections add aws_credentials --conn-uri 'aws://******************************'
#
#
# TO-DO: run the follwing command and observe the JSON output: 
# airflow connections get redshift_default -o json
# 
# [{"id": "2", 
# "conn_id": "redshift_default", 
# "conn_type": "redshift", 
# "description": "", 
# "host": "default-workgroup.555413626884.us-east-1.redshift-serverless.amazonaws.com", 
# "schema": "dev", 
# "login": "admin", 
# "password": "***********", 
# "port": "5439", 
# "is_encrypted": "True", 
# "is_extra_encrypted": "True", 
# "extra_dejson": {"region_name": "us-east-1"}, 
# "get_uri": "redshift://admin:************@default-workgroup.555413626884.us-east-1.redshift-serverless.amazonaws.com:5439/dev"}]
#
# TO-DO: Update the following command with the URI and un-comment it:
#
airflow connections add redshift --conn-uri 'redshift://admin:**********@default-workgroup.555413626884.us-east-1.redshift-serverless.amazonaws.com:5439/dev'
#
# TO-DO: update the following bucket name to match the name of your S3 bucket and un-comment it:
#
airflow variables set s3_bucket wgu-udacity-d608-bjordan
#
# TO-DO: un-comment the below line:
#
airflow variables set s3_prefix data-pipelines