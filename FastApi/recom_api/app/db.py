#contraseñas fake porque las reales son otras.

import psycopg2

def get_connection():
    return psycopg2.connect(
        host="localhost",         
        dbname="Postgres",
        user="Postgres",
        password="admin123",
        port=5432
    )
