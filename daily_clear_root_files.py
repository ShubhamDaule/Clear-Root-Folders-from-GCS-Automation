import datetime
import logging
import os
import csv

from airflow import settings
from airflow.models import DAG, DagModel
from airflow.operators.python import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.email import send_email_smtp
from datetime import datetime, timedelta
from google.cloud import storage
from airflow.configuration import conf
from airflow.models import Variable

environment = Variable.get('platform_composer_env')
email_subject_prefix = "GCP "+environment+' '+"clear root files"
to_emails = ['your_email@example.com']  # Replace with appropriate email address
var3=conf.get("webserver", "web_server_name")

DAG_ID = 'daily_clear_root_files'
deleted_list = []

import importlib
import re

def report_failure(context):
	subject=f'{email_subject_prefix} Daily dag failed'
	html_content = f"""
                    Hello Team, <br>
                    <br>
                    The daily job {var3}.{DAG_ID} for clearing wrong file path folder in the bucket has been failed.<br>
                    <br>
                    Thanks<br>
		    		<br>Runbook:<br><href>link</href>
                    
                    """.format(context['task_instance_key_str'])
    #send_email.execute(context) 
	send_email_smtp(to_emails,subject,html_content)
    
default_args = {
		'owner':'airflow', 
		'depends_on_past':False, 
		'start_date':'Any Start Date',
		'email_on_failure':True,
		'on_failure_callback':report_failure, 
		"email_subject_prefix":email_subject_prefix,
		'retries': 2,
    	'retry_delay': timedelta(minutes=5),}

dag = DAG(DAG_ID, default_args=default_args, schedule_interval="0 18 * * *")

def daily_clear_root_files():
	var1=conf.get("webserver", "web_server_name")
	var2=conf.get("logging", "remote_base_log_folder")
	var2=var2.split('/')[2]
	session = settings.Session()

	dags = session.query(DagModel).all()
	deleted_list.append({'folder_path': 'Folder Path', 'environment':'Environment Name'})
	
	storage_client = storage.Client()
	bucket_name=var2
	bucket = storage_client.bucket(bucket_name)
	logging.info(var2)
	all_blobs = storage_client.list_blobs(bucket_name)

	blobs = []
	for blob in all_blobs:
		num = blob.name.count('/')
		folder_name = blob.name.split('/') 
		ind_delete=0
		for i in folder_name:
			if i == '':
				ind_delete=1
		if(ind_delete==1 and folder_name[num] != ''):
			deleted_list.append({'folder_path': blob.name, 'environment': var1})

	logging.info(len(deleted_list))
	for j in range(len(deleted_list)):
		file_name=deleted_list[j].get('folder_path')
		if(file_name!='Folder Path'):
			blob1=bucket.get_blob(file_name)
			logging.info(blob1.name)
			blob1.delete()
		logging.info('folder to be deleted   ' + file_name)	

	keys = deleted_list[0].keys()
	storage_client = storage.Client()
	if Variable.get('platform_composer_env')=='preprod':
		env_bucket='prod'
	else:
		env_bucket=Variable.get('platform_composer_env')
	bucket_name = "Your Bucket Name"+env_bucket
	file_name = "deleted_root_files_"
	ct1 = datetime.now()
	outfile = 'composer/jobs'+'/'+var1+'/'+DAG_ID+'/'+file_name+str(ct1.strftime("%Y%m%d%H%M%S")) + '.csv'
	bucket = storage_client.bucket(bucket_name)
	blob = bucket.blob((outfile))
	with blob.open(mode='w') as output_file:
		dict_writer = csv.DictWriter(output_file,keys,delimiter=";")
		#dict_writer.writerheader()
		dict_writer.writerows(deleted_list)

clear_root_files = PythonOperator(task_id = 'clear_root_files', python_callable=daily_clear_root_files, dag=dag, provide_context=True)