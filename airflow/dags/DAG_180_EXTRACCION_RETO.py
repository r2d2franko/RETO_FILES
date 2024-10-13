from __future__ import annotations
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.decorators import task, dag
import time 
from sqlalchemy.sql import text
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
from sqlalchemy.orm import relationship, sessionmaker,scoped_session

server="192.168.1.66\SQLEXPRESS"
username="sa"
password="LPfnA4367"
database="Staging"
connection_string = 'DRIVER={ODBC Driver 18 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password+';TrustServerCertificate=yes;'
connection_url    = sa.engine.URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
engine            = sa.create_engine(connection_url)
dwhConnection = engine.connect()
Session = scoped_session(sessionmaker(bind=engine))

with DAG(
    "DAG_180_EXTRACCION_RETO",
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
        #pdf_files=pdf_files.iloc[:5]
        pdf_files=pdf_files['file'].tolist() 
        
        main_dataframe = pd.DataFrame(pd.read_csv(base_path/data_dir/pdf_files[0])) 
        for i in range(1,len(pdf_files)): 
            data = pd.read_csv(base_path/data_dir/pdf_files[i]) 
            df = pd.DataFrame(data) 
            main_dataframe=  pd.concat([main_dataframe,df])         
        
        insert_extrat(df_datos=main_dataframe)

    # [END extract_function]
    def insert_extrat(df_datos=pd.DataFrame):
        with Session() as session:   
            df_datos.to_sql("#temp_table", session.connection(), if_exists="replace", index=False)      
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
            session.execute(query)
            session.commit()
        #with engine.begin() as connection:
            #df_datos.to_sql(name='Datos', con=connection, if_exists='replace', index=False)
    def conexion_insert_extrat():
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
    def transform(**kwargs):
        ti = kwargs["ti"]           
        var_fecha='2013-01-01'        
        query = f""" SELECT * FROM [Staging].[dbo].[Datos] 
                WHERE date >= '{var_fecha}'"""
        engine=conexion_insert_transform()
        with Session() as session:
            pdf_datos = pd.read_sql_query(sa.text(query), session.connection())
            session.commit()
            
        pdf_datos['date'] = pd.to_datetime(pdf_datos['date'])
        pdf_datos['year'] =(pdf_datos['date'].dt.year)
        pdf_datos['month'] =(pdf_datos['date'].dt.month)
        pdf_datos['week'] =(pdf_datos['date'].dt.isocalendar().week)
        pdf_datos['year_week'] = pdf_datos['year'].astype(str)+ pdf_datos['month'].astype(str).str.zfill(2)+pdf_datos['week'].astype(str).str.zfill(2)
    
        df_agg = pdf_datos.groupby([pdf_datos['year_week'],pdf_datos['Name'],pdf_datos['volume']]).agg(        
            media_open=('open', np.mean),
            media_close=('close', np.mean),
            media_high=('high', np.mean),
            media_low=('low', np.mean))
        df_agg = df_agg.reset_index()
        df_agg['volume']=df_agg['volume'].apply(str)
        insert_transform(engine,df_agg)


    # [END extract_function]
    def insert_transform(engine,df_datos=pd.DataFrame):    
        with Session() as session: 
            #df_datos['volume']=df_datos['volume'].apply(str)
            df_datos.to_sql("#temp_aggtable", session.connection(), if_exists="replace", index=False)            
            query="""\
            MERGE [DM].[dbo].[FACTVOLUMENAMEMEDIA] WITH (HOLDLOCK) AS main
            USING (SELECT [Name],[volume],[media_open],[media_close],[media_high],[media_low],[year_week]  FROM #temp_aggtable) AS temp
            ON (main.[year_week] = temp.[year_week] and main.[volume] = temp.[volume] and main.[Name] = temp.[Name])
            WHEN MATCHED THEN
            UPDATE SET 
                main.[media_open] = temp.[media_open], 
                main.[media_high] =  temp.[media_high],
                main.[media_low] =  temp.[media_low],
                main.[media_close] =  temp.[media_close],
                main.[volume] =  temp.[volume],
                main.[created_at] =  getdate()
            WHEN NOT MATCHED THEN
            INSERT ([Name],[volume],[media_open],[media_close],[media_high],[media_low],[year_week] ) VALUES (temp.[Name],temp.[volume], temp.[media_open], temp.[media_close],temp.[media_high],temp.[media_low],temp.[year_week]);
            """
            session.execute(query)
            session.commit()
    def conexion_insert_transform():
        server="192.168.1.66\SQLEXPRESS"
        username="sa"
        password="LPfnA4367"
        database="DM"
        connection_string = 'DRIVER={ODBC Driver 18 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password+';TrustServerCertificate=yes;'
        connection_url    = sa.engine.URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
        engine            = sa.create_engine(connection_url)
        return engine
    # [START main_flow]
    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    transform_task.doc_md = textwrap.dedent(
        """\
    #### Extract task
    *Lectura de archivos csv con datos para crear un DataFrame el cual sera enviado a SQL
    *Merge de informacion update/create
    *Insercion a FACT
    """
    )
    


    

extract_task >> transform_task
    


      


 
