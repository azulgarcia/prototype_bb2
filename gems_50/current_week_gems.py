import os
import pandas as pd
from cb_database_connection import open_connection
'''
directorio = 'data'

dataframes = []

for archivo in os.listdir(directorio):
    if archivo.endswith('.csv'):
        ruta_archivo = os.path.join(directorio, archivo)
        df = pd.read_csv(ruta_archivo)
        dataframes.append(df)

df = pd.concat(dataframes, ignore_index=True)
'''
df = pd.read_csv('data/all_projects_api_52_2024.csv')

current_week = 1

df['year'] = 2024

df = df[['slug', 'year', 'week']]

connection = open_connection()

for index, row in df.iterrows():
    slug = row['slug']
    year = row['year']
    week = row['week']

    try:
        cursor = connection.cursor(dictionary=True)
        sql_select_query = 'select id, name from projects where slug = %s;'
        cursor.execute(sql_select_query, (slug,))
        result = cursor.fetchall()
        if result:
            df.at[index, 'id_project'] = result[0]['id']
            df.at[index, 'name_project'] = result[0]['name']
            print(index, row['week'], row['slug'], result[0]['id'], result[0]['name'])
    finally:
        cursor.close()

df['init_price'] = ''
df['end_price'] = ''
df['performance'] = ''
df['date'] = ''


for index, row in df.iterrows():
    id_project = row['id_project']
    year = row['year']
    week = row['week']

    try:
        cursor = connection.cursor(dictionary=True)
        sql_select_query = 'select * from prices where year(created_at)=%s and week(created_at,3)=%s ' \
                           'and id_project = %s;'
        cursor.execute(sql_select_query, (2023, 52, id_project))
        init_price = cursor.fetchall()
        df.at[index, 'init_price'] = init_price[0]['price']

        cursor.execute(sql_select_query, (year, 1, id_project))
        end_price = cursor.fetchall()
        df.at[index, 'date'] = end_price[0]['created_at']

        df.at[index, 'end_price'] = end_price[0]['price']

        performance = (end_price[0]['price'] - init_price[0]['price'])/init_price[0]['price']
        df.at[index, 'performance'] = performance

        print(index, row['week'], row['slug'], row['id_project'], row['name_project'],
              end_price[0]['created_at'], init_price[0]['price'], end_price[0]['price'], performance)
    finally:
        cursor.close()

df['category'] = 'Gems<50mm'
df['week'] = df['week'] + 1
df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')

df = df[['category', 'date', 'year', 'week', 'id_project', 'name_project', 'init_price', 'end_price',
         'performance']]

df.to_csv(f'data/gems_week_{current_week}.csv', index=False)

print(df)