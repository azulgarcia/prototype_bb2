import pandas as pd
from cb_database_connection import open_connection

df = pd.read_csv('../data/projects_bb2_general_week_10.csv')

df['date'] = pd.to_datetime(df['date'])
df['year'] = df['date'].dt.strftime('%Y')
df['week'] = df['date'].dt.strftime('%W')

year = '2024'
current_week = '10'

print(df)

filter_df = df[(df['year'] == year) & (df['week'] == current_week)]

df_performances = []

connection = open_connection()

for index, row in filter_df.iterrows():
    id = row['id_project']
    year = int(row['year'])
    week = int(row['week'])
    date = row['date']
    name = row['name_project']
    rank = row['rank']
    try:
        cursor = connection.cursor(dictionary=True)
        query_init_price = "select price, created_at from prices " \
                           "where id_project = %s and year(created_at) = %s and week(created_at,3) = %s;"
        cursor.execute(query_init_price, (id, year, week,))
        price_init_week = cursor.fetchall()
        init_price = float(price_init_week[0]['price'])

        df_performances.append({
                         'date': date,
                         'year': year,
                         'week': week,
                         'id_project': id,
                         'project_name': name,
                         'start_price': init_price,
                         'rank': rank,
                         })
        print(name, id, year, week, date, init_price)
    except:
        print(f'{year}, {week}, Project: {name},: has prices problems')

    finally:
        cursor.close()

df_performances = pd.DataFrame(df_performances)
df_performances.to_csv('data/bb2_general_current_week.csv', sep=',', index=False)