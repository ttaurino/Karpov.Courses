import requests
import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime
from io import StringIO
import telegram

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'

default_args = {
    'owner': 'a.batalov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 10, 7),
}
schedule_interval = '0 12 * * *'

CHAT_ID = 0
try:
    BOT_TOKEN = Variable.get('telegram_secret')
except:
    BOT_TOKEN = ''


def send_message(context):
    date = context['ds']
    dag_id = context['dag'].dag_id
    message = f'Huge success! Dag {dag_id} completed on {date}'
    if BOT_TOKEN != '':
        bot = telegram.Bot(token=BOT_TOKEN)
        bot.send_message(chat_id=CHAT_ID, message=message)
    else:
        pass

@dag(default_args=default_args, catchup=False, schedule_interval=schedule_interval)
def top_10_airflow_2():
    @task(retries=3)
    def get_data():
        # Оставили csv для совместимости со старым кодом
        top_doms = pd.read_csv(TOP_1M_DOMAINS)
        top_data = top_doms.to_csv(index=False)
        return top_data

    @task(retries=4, retry_delay=timedelta(10))
    def get_table_ru(top_data):
        top_data_df = pd.read_csv(StringIO(top_data), names=['rank', 'domain'])
        top_data_ru = top_data_df[top_data_df['domain'].str.endswith('.ru')]
        return top_data_ru.to_csv(index=False)

    @task()
    def get_stat_ru(top_data_ru):
        ru_df = pd.read_csv(StringIO(top_data_ru))
        ru_avg = int(ru_df['rank'].aggregate(np.mean))
        ru_median = int(ru_df['rank'].aggregate(np.median))
        return {'ru_avg': ru_avg, 'ru_median': ru_median}

    @task()
    def get_table_com(top_data):
        top_data_df = pd.read_csv(StringIO(top_data), names=['rank', 'domain'])
        top_data_com = top_data_df[top_data_df['domain'].str.endswith('.com')]
        return top_data_com.to_csv(index=False)

    @task()
    def get_stat_com(top_data_com):
        com_df = pd.read_csv(StringIO(top_data_com))
        com_avg = int(com_df['rank'].aggregate(np.mean))
        com_median = int(com_df['rank'].aggregate(np.median))
        return {'com_avg': com_avg, 'com_median': com_median}

    @task(on_success_callback=send_message)
    def print_data(ru_stat, com_stat):

        context = get_current_context()
        date = context['ds']
        print(f'''Data from .RU for {date}
                  Avg rank: {ru_avg}
                  Median rank: {ru_median}''')

        print(f'''Data from .COM for {date}
                          Avg rank: {com_avg}
                          Median rank: {com_median}''')

    top_data = get_data()
    top_data_ru = get_table_ru(top_data)
    ru_data = get_stat_ru(top_data_ru)

    top_data_com = get_table_com(top_data)
    com_data = get_stat_com(top_data_com)

    print_data(ru_data, com_data)

top_10_airflow_2 = top_10_airflow_2()