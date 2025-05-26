import streamlit as st

import json

import re

from datetime import datetime, timedelta

from pathlib import Path



# ---------------- Page Setup ----------------

st.set_page_config(page_title="ICS Egress Framework", layout="wide")

st.title("ICS Egress Framework")



# ---------------- Login ----------------

if "authenticated" not in st.session_state:

    st.session_state.authenticated = False



if not st.session_state.authenticated:

    st.subheader("Login")

    password = st.text_input("Enter Password", type="password")

    if st.button("Submit"):

        if password == "icsegf2025":

            st.session_state.authenticated = True

        else:

            st.error("Incorrect password.")

    st.stop()



# ---------------- Tabs ----------------

tabs = st.tabs(["Code Preview", "SQL Config", "Generated SQL", "Execution JSON", "Generated Export Job", "Generated DAG"])

code_tab, sql_config_tab, sql_tab, execution_tab, export_tab, dag_tab = tabs



# ---------------- Helper Functions ----------------

def extract_sql_components(script_text):

    # Extracting basic SQL elements

    select = re.search(r"SELECT\s+(.*?)\s+FROM", script_text, re.IGNORECASE | re.DOTALL)

    where = re.search(r"WHERE\s+(.*?)(GROUP BY|ORDER BY|LIMIT|$)", script_text, re.IGNORECASE | re.DOTALL)

    with_ = re.search(r"WITH\s+(.*?)\s+SELECT", script_text, re.IGNORECASE | re.DOTALL)

    interval = re.search(r"INTERVAL\s+(\d+)\s+(HOUR|DAY|MINUTE)", script_text, re.IGNORECASE)



    columns = select.group(1).strip() if select else "*"

    where_clause = where.group(1).strip() if where else ""

    with_clause = with_.group(1).strip() if with_ else ""

    schedule = "0 * * * *" if interval else "0 3 * * *"

    

    tables = list(set(re.findall(r'\bFROM\s+([a-zA-Z0-9_.]+)', script_text, re.IGNORECASE)))

    return columns, where_clause, with_clause, schedule, tables



def generate_export_script(sql, destination_path):

    return f'''from google.cloud import bigquery

import pandas as pd



client = bigquery.Client()

sql_query = """{sql}"""

df = client.query(sql_query).to_dataframe()



if df.empty:

    print("No data to export.")

else:

    df.to_csv("{destination_path}", index=False)

    print("Data exported successfully to {destination_path}")

'''



def generate_dag_script(job_name, export_filename, execution_json):

    return f'''from airflow import DAG

from airflow.operators.python import PythonOperator

from airflow.operators.python import ShortCircuitOperator

from datetime import datetime, timedelta

import os

import subprocess



def check_export_exists():

    return os.path.exists("{export_filename}")



def execute_logic():

    try:

        result = subprocess.run("{execution_json['command_logic']}", shell=True, check=True)

        print("Execution successful.")

    except subprocess.CalledProcessError as e:

        print("Execution failed:", e)

        raise



default_args = {{

    'owner': 'airflow',

    'retries': {execution_json['retries']},

    'retry_delay': timedelta(minutes={execution_json['delay_minutes']})

}}



with DAG(

    dag_id="{job_name}_dag",

    default_args=default_args,

    schedule_interval="{execution_json['schedule']}",

    start_date=datetime(2024, 1, 1),

    catchup=False

) as dag:



    check_file = ShortCircuitOperator(

        task_id='check_export_script',

        python_callable=check_export_exists

    )



    run_job = PythonOperator(

        task_id='execute_export_job',

        python_callable=execute_logic

    )



    check_file >> run_job

'''



# ---------------- File Upload ----------------

with code_tab:

    st.subheader("Upload egress job script")

    uploaded_file = st.file_uploader("Upload .sql, .bteq, .txt, .py, .sh, .java, .cs", type=["sql", "bteq", "txt", "py", "sh", "java", "cs"])

    if uploaded_file:

        script_text = uploaded_file.read().decode("utf-8")

        st.text_area("Code Preview", script_text, height=300)

        st.session_state.script = script_text

        st.session_state.job_name = Path(uploaded_file.name).stem



# ---------------- SQL Config Tab ----------------

if "script" in st.session_state:

    with sql_config_tab:

        cols, where, with_, sched, tables = extract_sql_components(st.session_state.script)

        sql_config = {

            "job_name": st.session_state.job_name,

            "source_tables": tables,

            "columns": cols,

            "with_clause": with_,

            "where_clause": where,

            "sql_logic": st.session_state.script.strip()

        }

        st.session_state.sql_config = sql_config

        st.subheader("SQL Config")

        st.json(sql_config)

        st.download_button("Download SQL Config", json.dumps(sql_config, indent=2), file_name="sql_config.json")



# ---------------- SQL Logic ----------------

if "sql_config" in st.session_state:

    with sql_tab:

        st.subheader("Generated SQL Logic")

        sql = st.session_state.sql_config['sql_logic']

        st.code(sql, language="sql")

        st.download_button("Download SQL", sql, file_name="generated_query.sql")



# ---------------- Execution JSON ----------------

    with execution_tab:

        exec_json = {

            "job_name": st.session_state.job_name,

            "execution_condition": ["if errorcode <> 0 then .quit 1;"],

            "command_logic": f"python {st.session_state.job_name}_export.py",

            "schedule": sched,

            "retries": 1,

            "delay_minutes": 5

        }

        st.session_state.exec_json = exec_json

        st.subheader("Execution JSON")

        st.json(exec_json)

        st.download_button("Download Execution JSON", json.dumps(exec_json, indent=2), file_name="execution.json")



# ---------------- Export Job Script ----------------

    with export_tab:

        st.subheader("Generated Export Job (Python)")

        export_path = f"/sftp/{st.session_state.job_name}_data.csv"

        export_code = generate_export_script(sql, export_path)

        st.code(export_code, language="python")

        st.download_button("Download Export Script", export_code, file_name=f"{st.session_state.job_name}_export.py")



# ---------------- DAG Generation ----------------

    with dag_tab:

        st.subheader("Generated Airflow DAG")

        dag_code = generate_dag_script(st.session_state.job_name, f"{st.session_state.job_name}_export.py", st.session_state.exec_json)

        st.code(dag_code, language="python")

        st.download_button("Download DAG", dag_code, file_name=f"{st.session_state.job_name}_dag.py")