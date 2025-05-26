#import psycopg2
#import os

#def get_connection():
    #return psycopg2.connect(
        #host=os.getenv("DB_HOST"),
        #dbname=os.getenv("DB_NAME"),
        #user=os.getenv("DB_USER"),
        #password=os.getenv("DB_PASSWORD"),
        #port=os.getenv("DB_PORT", 5432)
    )

#export DB_HOST="34.133.184.186"
#export DB_NAME="postgres"
#export DB_USER="postgres"
#export DB_PASSWORD="admin"
#export DB_PORT=5432

import psycopg2

def get_connection():
    return psycopg2.connect(
        host="localhost",          # Usar "localhost" si estás en la misma VM donde corre PostgreSQL
        dbname="postgres",
        user="postgres",
        password="admin",
        port=5432
    )
