from airflow import DAG
from airflow.operators.python import PythonOperator
from plugins.operators.zip import UnzipOperator
from datetime import datetime
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from include.extract.XML_to_Parquet import convert

with DAG("abn_new",start_date=datetime(2024,4,1),catchup=False) as dag:
    Unzip_xml_part1=UnzipOperator(
        task_id='unzip_xml_part1',
        path_to_zip_file="/usr/local/airflow/include/datasets/public_split_1_10.zip",
        path_to_unzip_contents="/usr/local/airflow/include/datasets"
    )

    Unzip_xml_part2=UnzipOperator(
        task_id='unzip_xml_part2',
        path_to_zip_file="/usr/local/airflow/include/datasets/public_split_11_20.zip",
        path_to_unzip_contents="/usr/local/airflow/include/datasets"
    )

    upload_csv_to_gcs=LocalFilesystemToGCSOperator(
        task_id='upload_csv_to_gcs',
        src='include/datasets/business_names_202404.csv',
        dst='business_names.csv',
        bucket='abn_bucket',
        gcp_conn_id='gcp',
        mime_type='text/csv'
    )

    Change_file_name = BashOperator(
    task_id='change_file_name',
    bash_command='cd /usr/local/airflow/include/datasets && for file in *.xml; do new_filename="${file:9}" && mv "$file" "$new_filename"; done'
    )

    convert_xml_to_parquet=PythonOperator(
        task_id='convert_xml_to_parquet',
        python_callable=convert
    )

    create_abn_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_abn_dataset',
        dataset_id='abn',
        gcp_conn_id='gcp'
    )

[Unzip_xml_part1,Unzip_xml_part2] >> Change_file_name >> convert_xml_to_parquet



