from __future__ import annotations
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.decorators import task, dag
import time 
from datetime import datetime, timedelta
import pyodbc
from smbprotocol.connection import Connection
from smbprotocol.session import Session
from smbprotocol.tree import TreeConnect
import os
import uuid
import pandas as pd
import numpy as np
from pathlib import Path
import sqlalchemy as sa
import pendulum
import textwrap
import json
from io import StringIO
with DAG(
    "ONCE_DAG_180_EXTRACCION_RETO",
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={"retries": 2,"email_on_failure": True,"email": "noreply@astronomer.io"},
    # [END default_args]
    description="DAG RETO ET1",
    schedule=None,
    start_date=pendulum.datetime(2024, 10, 12, tz="UTC"),
    catchup=False,
    tags=["RETO"]
) as dag:
    # [END instantiate_dag]
    # [START documentation]
    dag.doc_md = __doc__
    # [END documentation]
    # [START extract_function]
    def extract(**kwargs):
        ti = kwargs["ti"]
        base_path = Path("/opt/airflow/dags")
        data_dir = Path("data")

        files = os.listdir(base_path/data_dir)
        pdf_files           = pd.DataFrame({'file': files})
        pdf_files['tipo'] = np.select([pdf_files['file'].str.contains('.csv')],'1',default = 0)
        pdf_files=pdf_files[pdf_files['tipo']=='1']
        pdf_files=pdf_files.iloc[:5]
        pdf_files=pdf_files['file'].tolist() 
        
        main_dataframe = pd.DataFrame(pd.read_csv(base_path/data_dir/pdf_files[0])) 
        for i in range(1,len(pdf_files)): 
            data = pd.read_csv(base_path/data_dir/pdf_files[i]) 
            df = pd.DataFrame(data) 
            main_dataframe=  pd.concat([main_dataframe,df])         
        ti.xcom_push("new_data", main_dataframe)


    # [END extract_function]
    def insert(**kwargs):
        ti = kwargs["ti"]        
        df_datos = ti.xcom_pull(task_ids="extract", key="new_data")     
        engine=conexion()
        with engine.begin() as connection:
            df_datos.to_sql("#temp_table", connection, if_exists="replace", index=False)
            query="""\
            MERGE Staging.dbo.Datos WITH (HOLDLOCK) AS main
            USING (SELECT [date],[open],[high],[low],[close],[volume],[Name]  FROM #temp_table) AS temp
            ON (main.date = temp.date and main.volume = temp.volume and main.name = temp.name)
            WHEN MATCHED THEN
            UPDATE SET 
                main.[open] = temp.[open], 
                main.[high] =  temp.[high],
                main.[low] =  temp.[low],
                main.[close] =  temp.[close],
                main.[created_at] =  getdate()
            WHEN NOT MATCHED THEN
            INSERT ([date],[open],[high],[low],[close],[volume],[Name] ) VALUES (temp.date, temp.[open], temp.high,temp.low,temp.[close],temp.volume,temp.Name);
            """
            connection.exec_driver_sql(query)
        #with engine.begin() as connection:
             #df_datos.to_sql(name='Datos', con=connection, if_exists='replace', index=False)
    

    def conexion():
        server="192.168.1.66\SQLEXPRESS"
        username="sa"
        password="LPfnA4367"
        database="Staging"
        connection_string = 'DRIVER={ODBC Driver 18 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password+';TrustServerCertificate=yes;'
        connection_url    = sa.engine.URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        engine            = sa.create_engine(connection_url)
        
        return engine

    # [START main_flow]
    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    extract_task.doc_md = textwrap.dedent(
        """\
    #### Extract task
    *Lectura de archivos csv con datos para crear un DataFrame el cual sera enviado a SQL
    *Merge de informacion update/create
    *Insercion a FACT
    """
    )
    
    insert_task = PythonOperator(
        task_id="insert",
        python_callable=insert,
    )

    insert_task.doc_md = textwrap.dedent(
        """\
    #### Insert task
    Insercion de datos en SQL SERVER(LOCAL) en la tabla de sandbox, para iniciar transformacion
    """
    )

    extract_task  >> insert_task

 
