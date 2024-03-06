import pandas as pd
from cb_database_connection import open_connection

df = pd.read_csv('data/projects_bb2_general.csv')

df['date'] = pd.to_datetime(df['date'])
df['year'] = df['date'].dt.strftime('%Y')
df['week'] = df['date'].dt.strftime('%W')

df_performances = []

connection = open_connection()

for index, row in df.iterrows():
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

        query_end_price = "select price, created_at from prices " \
                           "where id_project = %s and year(created_at) = %s and week(b.created_at,3) = %s;"

        if week == 52:
            cursor.execute(query_init_price, (id, year+1, 1,))
        else:
            cursor.execute(query_init_price, (id, year, week + 1,))

        price_end_week = cursor.fetchall()
        end_price = float(price_end_week[0]['price'])

        performance = (end_price - init_price) / init_price

        df_performances.append({
                         'date': date,
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
        print(f'{year}, {week}, Project: {name},: has prices problems')

    finally:
        cursor.close()

df_performances = pd.DataFrame(df_performances)
df_performances.to_csv('data/bb2_general_performances.csv', sep=',', index=False)