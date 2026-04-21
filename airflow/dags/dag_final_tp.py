from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

default_args = {
    "owner": "valen",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag_final_tp",
    default_args=default_args,
    description="Pipeline final TP",
    schedule="@daily",
    start_date=datetime(2026, 4, 20),
    catchup=False,
    tags=["tp", "final"],
) as dag:

    filter_data = BashOperator(
        task_id="filter_data",
        bash_command="python3 $AIRFLOW_HOME/tasks/filter_data.py",
    )

    top_ctr = BashOperator(
        task_id="top_ctr",
        bash_command="python3 $AIRFLOW_HOME/tasks/top_ctr.py",
    )

    top_product = BashOperator(
        task_id="top_product",
        bash_command="python3 $AIRFLOW_HOME/tasks/top_product.py",
    )

    db_writing = BashOperator(
        task_id="db_writing",
        bash_command="python3 $AIRFLOW_HOME/tasks/db_writing.py",
    )

    filter_data >> [top_ctr, top_product]
    [top_ctr, top_product] >> db_writing
