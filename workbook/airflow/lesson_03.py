import requests
import pandas as pd
import numpy as np
import telegram
from airflow.models import Variable
from io import StringIO
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'
try:
    BOT_TOKEN = Variable.get('telegram_secret')
except:
    BOT_TOKEN = ''
BOT_CHAT = 0

default_args = {
    'owner': 'a.batalov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2021, 10, 7),
}

schedule_interval = '0 12 * * *'

def send_message(context):
    if BOT_TOKEN:
        bot = telegram.Bot(token=BOT_TOKEN)
        dag_id = context['dag'].dag_id
        message = f'Huge success. Dag {dag_id} completed'
        bot.send_message(chat_id=BOT_CHAT, text=message)
    else:
        pass


@dag(default_args=default_args, catchup=False, schedule_interval=schedule_interval)
def new_top_10_new():
    @task()
    def get_top_ten():
        # Оставили csv для совместимости со старым кодом
        top_doms = pd.read_csv(TOP_1M_DOMAINS)
        top_data = top_doms.to_csv(index=False)
        return top_data

    @task()
    def get_ru_df(top_data):
        top_data_df = pd.read_csv(StringIO(top_data), names=['rank', 'domain'])
        top_data_ru = top_data_df[top_data_df['domain'].str.endswith('.ru')]
        return top_data_ru.to_csv(index=False, header=False)

    @task()
    def get_ru_stat(ru_df):
        top_ru_df = pd.read_csv(StringIO(ru_df), names=['rank', 'domain'])
        top_data_ru_avg = int(top_ru_df['rank'].aggregate(np.mean))
        top_data_ru_median = int(top_ru_df['rank'].aggregate(np.median))
        return {'ru_avg': top_data_ru_avg, 'ru_median': top_data_ru_median}

    @task()
    def get_com_df(top_data):
        top_data_df = pd.read_csv(StringIO(top_data), names=['rank', 'domain'])
        top_data_com = top_data_df[top_data_df['domain'].str.endswith('.com')]
        return top_data_com.to_csv(index=False, header=False)

    @task()
    def get_com_stat(com_df):
        top_com_df = pd.read_csv(StringIO(com_df), names=['rank', 'domain'])
        top_data_com_avg = int(top_com_df['rank'].aggregate(np.mean))
        top_data_com_median = int(top_com_df['rank'].aggregate(np.median))
        return {'com_avg': top_data_com_avg, 'com_median': top_data_com_median}

    @task(on_success_callback=send_message)
    def print_data(ru_stat, com_stat):
        context = get_current_context()
        date = context['ds']
        ru_median_rank, ru_avg_rank = ru_stat['ru_median'], ru_stat['ru_avg']
        com_median_rank, com_avg_rank = com_stat['com_median'], com_stat['com_avg']

        data_ru = f'''domains in .RU for date {date}
                  median rank: {ru_median_rank}
                  avg rank: {ru_avg_rank}'''

        data_com = f'''domains in .RU for date {date}
                          median rank: {com_median_rank}
                          avg rank: {com_avg_rank}'''
        return {'data_ru': data_ru, 'data_com': data_com}


    top_data = get_top_ten()

    ru_df = get_ru_df(top_data)
    com_df = get_com_df(top_data)

    ru_stat = get_ru_stat(ru_df)
    com_stat = get_com_stat(com_df)

    print_data(ru_stat, com_stat)
new_top_10_new = new_top_10_new()