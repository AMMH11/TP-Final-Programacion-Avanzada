from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from datetime import datetime
import pandas as pd
import os
from google.cloud import storage
import psycopg2

BUCKET_NAME = "adtech-tp-data"
RAW_FOLDER = "raw"
GCP_PROJECT = "hybrid-subject-457721"

def descargar_y_procesar_data(**context):
    fecha_str = context["ds"]
    fecha = datetime.strptime(fecha_str, "%Y-%m-%d").date()

    storage_client = storage.Client(project=GCP_PROJECT)
    bucket = storage_client.bucket(BUCKET_NAME)

    blobs = {
        "ads_views": f"{RAW_FOLDER}/ads_views.csv",
        "product_views": f"{RAW_FOLDER}/product_views.csv"
    }

    for key, blob_path in blobs.items():
        local_path = f"/tmp/{key}.csv"
        blob = bucket.blob(blob_path)
        blob.download_to_filename(local_path)

    ads = pd.read_csv("/tmp/ads_views.csv")
    products = pd.read_csv("/tmp/product_views.csv")

    ads = ads[ads["date"] == fecha_str]
    products = products[products["date"] == fecha_str]

    if not ads.empty:
        grouped = ads.groupby(["advertiser_id", "product_id"])["type"].value_counts().unstack(fill_value=0)
        grouped["score"] = grouped.get("click", 0) / (grouped.get("impression", 0) + grouped.get("click", 0))
        top_ctr = grouped.reset_index()[["advertiser_id", "product_id", "score"]]
        top_ctr["fecha"] = fecha
    else:
        top_ctr = pd.DataFrame()

    if not products.empty:
        top_product = (
            products.groupby(["advertiser_id", "product_id"])
            .agg(views=("product_id", "count"))
            .reset_index()
        )
        top_product["fecha"] = fecha
    else:
        top_product = pd.DataFrame()

    context["ti"].xcom_push(key="top_ctr", value=top_ctr.to_dict(orient="records"))
    context["ti"].xcom_push(key="top_product", value=top_product.to_dict(orient="records"))

def insertar_en_postgres(**context):
    top_ctr_raw = context["ti"].xcom_pull(key="top_ctr")
    top_product_raw = context["ti"].xcom_pull(key="top_product")

    top_ctr = pd.DataFrame.from_records(top_ctr_raw) if top_ctr_raw else pd.DataFrame()
    top_product = pd.DataFrame.from_records(top_product_raw) if top_product_raw else pd.DataFrame()

    if top_ctr.empty and top_product.empty:
        return

    conn = psycopg2.connect(
        host="34.133.184.186",
        dbname="postgres",
        user="postgres",
        password="admin",
        port=5432
    )
    cursor = conn.cursor()

    for _, row in top_ctr.iterrows():
        cursor.execute(
            """
            INSERT INTO top_ctr (advertiser_id, product_id, score, fecha)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (advertiser_id, product_id, fecha) DO NOTHING
            """,
            (row["advertiser_id"], row["product_id"], row["score"], row["fecha"])
        )

    for _, row in top_product.iterrows():
        cursor.execute(
            """
            INSERT INTO top_product (advertiser_id, product_id, views, fecha)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (advertiser_id, product_id, fecha) DO NOTHING
            """,
            (row["advertiser_id"], row["product_id"], row["views"], row["fecha"])
        )

    conn.commit()
    cursor.close()
    conn.close()

with DAG(
    dag_id="dag_recomendaciones",
    start_date=datetime(2025, 5, 19),
    schedule_interval="@daily",
    catchup=True,
    tags=["adtech", "recomendaciones"]
) as dag:

    procesar = PythonOperator(
        task_id="procesar_data",
        python_callable=descargar_y_procesar_data,
        provide_context=True
    )

    cargar = PythonOperator(
        task_id="cargar_en_sql",
        python_callable=insertar_en_postgres,
        provide_context=True
    )

    procesar >> cargar
