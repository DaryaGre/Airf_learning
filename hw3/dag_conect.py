import pandas as pd

from airflow.hooks.base_hook import BaseHook

from util.deco import python_operator

from airflow.models import DAG



@python_operator()
def connection_operator(**context):
    hook = BaseHook.get_hook('airflow')

    con = context

    mean_fare_per_class_df = context['task_instance'].xcom_pull(task_ids="mean_fare_per_class", key='mean_fare_per_class_xcom_key')
    mean_fare_per_class_df = pd.read_json(mean_fare_per_class_df)
    mean_fare_per_class_table = f'mean_fare_per_class_{ con["dag_run"].run_id }'
    mean_fare_per_class_df.to_sql(mean_fare_per_class_table, hook.get_sqlalchemy_engine())


    pivot_dataset_df = context['task_instance'].xcom_pull(task_ids="pivot_dataset",
                                                                key='pivot_dataset_xcom_key')
    pivot_dataset_df = pd.read_json(pivot_dataset_df)
    pivot_dataset_table = f'pivot_dataset_{ con["dag_run"].run_id }'
    pivot_dataset_df.to_sql(pivot_dataset_table, hook.get_sqlalchemy_engine())
