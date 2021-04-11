import json
import datetime as dt
import pandas as pd

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.hooks.base_hook import BaseHook


default_args = {
    'owner': 'airflow',  # Информация о владельце DAG
    'start_date': dt.datetime(2020, 12, 23),  # Время начала выполнения пайплайна
    'retries': 1,  # Количество повторений в случае неудач
    'retry_delay': dt.timedelta(minutes=1),  # Пауза между повторами
    'depends_on_past': False,  # Запуск DAG зависит ли от успешности окончания предыдущего запуска по расписанию
}
@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2), tags=['download_titanic_dataset'])
def learning_dag_v2():

    @task()
    def download_titanic_dataset():
        url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
        df = pd.read_csv(url)
        df = df.to_json()

        return df

    @task()
    def pivot_dataset(titanic_df: dict):
        titanic_df = pd.read_json(titanic_df)
        df = titanic_df.pivot_table(index=['Sex'],
                                    columns=['Pclass'],
                                    values='Name',
                                    aggfunc='count').reset_index()

        df = df.to_json()

        return df

    @task()
    def mean_fare_per_class(titanic_df: dict):
        titanic_df = pd.read_json(titanic_df)
        df = titanic_df.groupby(['Pclass']).agg({'Fare': "mean"}).reset_index()

        df = df.to_json()

        return df

    @task()
    def load(mean_fare_per_class: dict,pivot_dataset: dict):
        hook = BaseHook.get_hook('airflow')

        mean_fare_per_class_df = pd.read_json(mean_fare_per_class)
        mean_fare_per_class_df.to_sql('mean_fare_per_class_table', hook.get_sqlalchemy_engine())


        pivot_dataset_df = pd.read_json(pivot_dataset)
        pivot_dataset_df.to_sql('pivot_dataset_table', hook.get_sqlalchemy_engine())


    order_data = download_titanic_dataset()
    pivot_dataset_df = pivot_dataset(order_data)
    mean_fare_per_class_df = mean_fare_per_class(order_data)
    load(mean_fare_per_class_df,pivot_dataset_df)
learning_dag = learning_dag_v2()