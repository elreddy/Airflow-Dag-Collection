# Importing required libraries
from airflow import DAG
from datetime import timedelta
from airflow.operators.bash_operator import BashOperator

# Defining arguments
default_arguments={
    'owner': 'Lokesh',
    'start_date': '2025-01-31',
    'email': ['abc@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Defining Dag
dag= DAG(
    "ETL_Toll_data",
    schedule_interval= timedelta(days=1),
    default_args= default_arguments,
    description='Apache Airflow Assignment',
)

# defining tasks

unzip_data= BashOperator(
    task_id='unzip_data',
    bash_command="""
         rm -rf /home/lokesh/files/*
         tar -xvzf /home/lokesh/tarfile/tolldata.tgz -C /home/lokesh/files
    """,
    dag=dag, 
)

extract_data_from_csv= BashOperator(
    task_id='extract_data_from_csv',
    bash_command="cut -d ',' -f1,2,3,4 /home/lokesh/files/vehicle-data.csv > /home/lokesh/files/csv_data.csv",
    dag=dag,
)

extract_data_from_tsv= BashOperator(
    task_id='extract_data_from_tsv',
    bash_command="awk -F '\t' '{print $5,$6,$7}' OFS=',' /home/lokesh/files/tollplaza-data.tsv > /home/lokesh/files/tsv_data.csv",
    dag=dag,
)

extract_data_from_fixed_width= BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command="awk '{print $10,$11}' OFS=',' /home/lokesh/files/payment-data.txt > /home/lokesh/files/fixed_width_data.csv",
    dag=dag,
)

consolidate_data= BashOperator(
    task_id='consolidate_data',
    bash_command="paste -d ',' /home/lokesh/files/csv_data.csv /home/lokesh/files/tsv_data.csv /home/lokesh/files/fixed_width_data.csv > /home/lokesh/files/extracted_data.csv",
    dag=dag,
)

transform_data= BashOperator(
    task_id='transform_data',
    bash_command="awk -F ',' '{ $4 = toupper($4); print }' OFS=',' /home/lokesh/files/extracted_data.csv > /home/lokesh/files/transformed_data.csv" ,
    dag=dag,
)

# Defining task dependencies
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data