#para corregir el punto levantado de seguridad en la exposicion de contraseñas dejamos un file generico
#y las reales estan en un archivo.env que no subimos al repositorio por motivos de seguridad.
import os
import psycopg2

def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=os.getenv("DB_PORT")
    )

