import os
import pymongo
from datetime import datetime
import pandas as pd
from app_ML_general.update.cb_database_connection import open_connection

user_mongo = os.environ.get('USER')
pass_mongo = os.environ.get('PASS')

client = pymongo.MongoClient(f"mongodb://{user_mongo}:{pass_mongo}@atlas-online-archive-63ac208cc7bb7126397aca29-jhrpz.a.query.mongodb.net/?ssl=true&authSource=admin")

db = client["BirdBrainDB"]

collection = db["predictionsBBGems2"]

start_date = datetime(2023, 8, 1)
end_date = datetime(2024, 3, 16)

query = {"date": {"$gte": start_date, "$lt": end_date}}

documents = collection.find(query)

# Crear listas para almacenar los datos
date_list = []
slug_list = []
rank_list = []

# Iterar sobre los documentos y extraer la información
for doc in documents:
    date = doc['date']
    predictions = doc['predictions']
    for prediction in predictions:
        date_list.append(date)
        slug_list.append(prediction['slug'])
        rank_list.append(prediction['rank'])

# Crear el DataFrame
data = {'date': date_list, 'slug': slug_list, 'rank': rank_list}
df = pd.DataFrame(data)
df['id_project'] = None
df['name_project'] = None

print(df)

client.close()

# Buscar proyectos
connection = open_connection()

for index, row in df.iterrows():
    #print(index, row['date'], row['slug'], row['rank'])
    try:
        cursor = connection.cursor(dictionary=True)
        sql_select_query = 'select id, name from projects where slug = %s;'
        cursor.execute(sql_select_query, (row['slug'],))
        result = cursor.fetchall()
        if result:
            df.at[index, 'id_project'] = result[0]['id']
            df.at[index, 'name_project'] = result[0]['name']
            print(index, row['date'], row['slug'], row['rank'], result[0]['id'], result[0]['name'])
    finally:
        cursor.close()

df.to_csv('data/projects_bb2_gems.csv', sep=',', index=False)