import pandas as pd
import requests
import json
from datetime import datetime, timedelta
import time

api_key = 'CG-1QXc2wiE6NQqxyRCZmN26ypp'

df = pd.read_csv('data/projects_bb2_general.csv')

df['date'] = pd.to_datetime(df['date'])
df['year'] = df['date'].dt.strftime('%Y')
df['week'] = df['date'].dt.strftime('%W')

df_performances = []

for index, row in df.iterrows():
    slug = row['slug']
    date_process = row['date']

    init_date = date_process
    end_date = init_date + timedelta(days=7)

    init_date_iso = init_date.strftime('%d-%m-%Y')
    end_date_iso = end_date.strftime('%d-%m-%Y')

    id = row['id_project']
    year = int(row['year'])
    week = int(row['week'])
    name = row['name_project']
    rank = row['rank']

    try:
        url_init_price = f'https://pro-api.coingecko.com/api/v3/coins/{slug}/history?date={init_date_iso}localization=false' \
                         f'&x_cg_pro_api_key={api_key}'
        response_api = requests.get(url_init_price)
        data_init_price = response_api.text
        parse_json_init = json.loads(data_init_price)
        init_price = parse_json_init['market_data']['current_price']['usd']

        url_end_price = f'https://api.coingecko.com/api/v3/coins/{slug}/history?date={end_date_iso}localization=false'
        response_api = requests.get(url_end_price)
        data_end_price = response_api.text
        parse_json_end = json.loads(data_end_price)
        end_price = parse_json_end['market_data']['current_price']['usd']

        performance = (end_price - init_price) / init_price

        df_performances.append({
            'date': date_process,
            'year': year,
            'week': week,
            'id_project': id,
            'project_name': name,
            'start_price': init_price,
            'end_price': end_price,
            'performance': performance,
            'rank': rank,
        })
        print(name, id, year, week, init_price, end_price, performance)

    except:
            print(f'Error coingecko at project {slug}, {date_process}')

df_performances = pd.DataFrame(df_performances)
df_performances.to_csv('data/bb2_general_performances_coingecko.csv', sep=',', index=False)