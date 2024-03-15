import os
import mysql.connector as db
from mysql.connector import Error

def open_connection():

    db_user = os.environ.get('MYSQL_USER')
    db_password = os.environ.get('MYSQL_PASSWORD')
    db_name = os.environ.get('MYSQL_DATABASE')
    db_host = os.environ.get('MYSQL_SERVER')
    db_port = os.environ.get('MYSQL_PORT') if os.environ.get('MYSQL_PORT') is not None else 3306
    
    try:

        return db.connect(user= db_user, passwd = db_password , host= db_host, database=db_name, port=db_port)

    except db.Error as error:
        
        print("failed to connect to database: {}".format(error))
        raise error
    
def close_connection(connection):
    
    if connection.is_connected():
        connection.close()

'''

if __name__ == '__main__':

    print('step1')
    print()
    connection = open_connection()
    print('connected to db')
    print()
    print('step 2')
    print()
    close_connection(connection)
    print('connection closed')
'''