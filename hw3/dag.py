import dag_def

from airflow.models import DAG
from util.settings import default_settings

with DAG(**default_settings()) as dag:
    dag_def.download_titanic_dataset() >> [dag_def.pivot_dataset(), dag_def.mean_fare_per_class()]