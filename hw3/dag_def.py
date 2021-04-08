
from util.deco import python_operator

import os
import pandas as pd
from airflow.hooks.base_hook import BaseHook

def get_path(file_name):
    return os.path.join(os.path.expanduser('~'), file_name)

@python_operator()
def download_titanic_dataset(**context):
    url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
    df = pd.read_csv(url)
    df = df.to_json()
    context['task_instance'].xcom_push('my_xcom_key', df)


@python_operator()
def pivot_dataset(**context):
    titanic_df = context['task_instance'].xcom_pull(task_ids="download_titanic_dataset", key='my_xcom_key')
    titanic_df = pd.read_json(titanic_df)
    df = titanic_df.pivot_table(index=['Sex'],
                                columns=['Pclass'],
                                values='Name',
                                aggfunc='count').reset_index()
    #hook = BaseHook.get_hook('airflow')

    #hook.insert_rows('t1', df, ['col1', 'col2'], 5, False) так не работает... (в других вариантах тоже), время на эксперименты кончилось...

    df.to_csv(get_path('titanic_pivot.csv'))

@python_operator()
def mean_fare_per_class(**context):
    titanic_df = context['task_instance'].xcom_pull(task_ids="download_titanic_dataset", key='my_xcom_key')
    titanic_df = pd.read_json(titanic_df)
    df = titanic_df.groupby(['Pclass']).agg({'Fare':"mean"}
    ).reset_index()

    #hook = BaseHook.get_hook('airflow')

    #hook.insert_rows('test1', df) так не работает... (в других вариантах тоже), время на эксперименты кончилось...

    df.to_csv(get_path('titanic_mean_fares.csv'))

