try:
    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from airflow.utils.dates import days_ago
    from datetime import datetime
    import sys
    sys.path.append('etl.py')
    from etl import extract, transformAndLoad
    print("All Dag modules are ok .....")
except Exception as e:
    print("Error {} ".format(e))


with DAG(
        dag_id="redditDag",
        schedule_interval="@daily",
        default_args={
            "owner": "airflow",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2023, 12, 11),
        },
        catchup=False) as f:
    
    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
        provide_context=True,
    )

    transform_load_task = PythonOperator(
        task_id="transformAndLoad",
        python_callable=transformAndLoad,
        op_args=[
        "{{ ti.xcom_pull(task_ids='extract') }}"
    ],
    )

    extract_task >> transform_load_task