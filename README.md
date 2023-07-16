# Daily-Clear-Root-Files-From-GCS-Bucket

This project is an Airflow DAG that automates the process of removing the root folders from the GCS bucket that cause errors. It uses the Google Cloud Storage Python API to list and delete the blobs in the bucket. It also generates a CSV file with the details of the deleted folders and uploads it to another bucket.

## Requirements
* Python 3.6 or higher
* Airflow 2.0 or higher
* Google Cloud Storage Python Client Library
* A GCP service account with appropriate permissions to access the buckets

## Installation
* Clone this repository to your local machine
* Copy the DAG file daily_clear_root_files.py to your Airflow DAGs folder
* Set the following Airflow variables in the UI or via CLI:
  * `platform_composer_env`: The name of the environment (e.g. preprod, prod)
  * `web_server_name`: The name of the web server
  * `remote_base_log_folder`: The name of the GCS bucket where the logs are stored
* Set the following environment variables in your Airflow environment:
  * `GOOGLE_APPLICATION_CREDENTIALS`: The path to the service account JSON file
  * `GOOGLE_CLOUD_PROJECT`: The ID of the GCP project


## Usage
* The DAG runs daily at 18:00 UTC
* It queries the DagModel table in the Airflow database to get the list of all DAGs
* It lists all the blobs in the GCS bucket where the logs are stored
* It deletes any blob that has an empty folder name or a folder name that does not match any DAG ID
* It creates a CSV file with the following columns: folder_path, environment
* It uploads the CSV file to another GCS bucket with the following path: composer/jobs/{environment}/{dag_id}/{file_name}_{timestamp}.csv
* It sends an email notification to a predefined list of recipients in case of failure
