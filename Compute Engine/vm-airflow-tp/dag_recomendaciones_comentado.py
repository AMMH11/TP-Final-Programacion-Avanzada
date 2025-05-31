from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import pandas as pd
from google.cloud import storage
import psycopg2

# Configuración general
BUCKET_NAME = "adtech-tp-data"
RAW_FOLDER = "raw"
GCP_PROJECT = "hybrid-subject-457721"

# Tarea 1: Filtrar datos por fecha y advertisers activos
def filtrar_datos_func(**context):
    fecha_str = context["ds"]
    storage_client = storage.Client(project=GCP_PROJECT)
    bucket = storage_client.bucket(BUCKET_NAME)

    bucket.blob(f"{RAW_FOLDER}/ads_views.csv").download_to_filename("/tmp/ads_views.csv")
    bucket.blob(f"{RAW_FOLDER}/product_views.csv").download_to_filename("/tmp/product_views.csv")
    bucket.blob(f"{RAW_FOLDER}/advertisers.csv").download_to_filename("/tmp/advertisers.csv")

    ads = pd.read_csv("/tmp/ads_views.csv")
    products = pd.read_csv("/tmp/product_views.csv")
    advertisers_df = pd.read_csv("/tmp/advertisers.csv")

    active_advertisers = advertisers_df[advertisers_df["status"] == "active"]["advertiser_id"].unique()

    ads = ads[(ads["date"] == fecha_str) & (ads["advertiser_id"].isin(active_advertisers))]
    products = products[(products["date"] == fecha_str) & (products["advertiser_id"].isin(active_advertisers))]

    context["ti"].xcom_push(key="ads_filtrados", value=ads.to_dict(orient="records"))
    context["ti"].xcom_push(key="products_filtrados", value=products.to_dict(orient="records"))

# Tarea 2: Calcular TopCTR (20 mejores por advertiser)
def top_ctr_func(**context):
    ads = pd.DataFrame.from_records(context["ti"].xcom_pull(key="ads_filtrados"))
    fecha = datetime.strptime(context["ds"], "%Y-%m-%d").date()

    if not ads.empty:
        grouped = ads.groupby(["advertiser_id", "product_id"])["type"].value_counts().unstack(fill_value=0)
        grouped["score"] = grouped.get("click", 0) / (grouped.get("impression", 0) + grouped.get("click", 0))
        top_ctr = grouped.reset_index()[["advertiser_id", "product_id", "score"]]
        top_ctr["fecha"] = fecha
        top_ctr = top_ctr.sort_values("score", ascending=False).groupby("advertiser_id").head(20)
    else:
        top_ctr = pd.DataFrame()

    context["ti"].xcom_push(key="top_ctr", value=top_ctr.to_dict(orient="records"))

# Tarea 3: Calcular TopProduct (20 más vistos por advertiser)
def top_product_func(**context):
    products = pd.DataFrame.from_records(context["ti"].xcom_pull(key="products_filtrados"))
    fecha = datetime.strptime(context["ds"], "%Y-%m-%d").date()

    if not products.empty:
        top_product = (
            products.groupby(["advertiser_id", "product_id"])
            .agg(views=("product_id", "count"))
            .reset_index()
        )
        top_product["fecha"] = fecha
        top_product = top_product.sort_values("views", ascending=False).groupby("advertiser_id").head(20)
    else:
        top_product = pd.DataFrame()

    context["ti"].xcom_push(key="top_product", value=top_product.to_dict(orient="records"))

# Tarea 4: Insertar resultados en PostgreSQL
def insertar_en_postgres(**context):
    top_ctr_raw = context["ti"].xcom_pull(key="top_ctr")
    top_product_raw = context["ti"].xcom_pull(key="top_product")

    top_ctr = pd.DataFrame.from_records(top_ctr_raw) if top_ctr_raw else pd.DataFrame()
    top_product = pd.DataFrame.from_records(top_product_raw) if top_product_raw else pd.DataFrame()

    if top_ctr.empty and top_product.empty:
        return
#las claves estan en un archivo.env que no se subio al repositorio para evitar problemas de seguridad
    conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    port=os.getenv("DB_PORT")
)
    cursor = conn.cursor()

    for _, row in top_ctr.iterrows():
        cursor.execute(
            "INSERT INTO top_ctr (advertiser_id, product_id, score, fecha) "
            "VALUES (%s, %s, %s, %s) "
            "ON CONFLICT (advertiser_id, product_id, fecha) DO NOTHING",
            (row["advertiser_id"], row["product_id"], row["score"], row["fecha"])
        )

    for _, row in top_product.iterrows():
        cursor.execute(
            "INSERT INTO top_product (advertiser_id, product_id, views, fecha) "
            "VALUES (%s, %s, %s, %s) "
            "ON CONFLICT (advertiser_id, product_id, fecha) DO NOTHING",
            (row["advertiser_id"], row["product_id"], row["views"], row["fecha"])
        )

    conn.commit()
    cursor.close()
    conn.close()

# Definición del DAG
with DAG(
    dag_id="dag_recomendaciones",
    start_date=datetime(2025, 5, 19),
    schedule_interval="@daily",
    catchup=True,
    tags=["adtech", "recomendaciones"]
) as dag:

    filtrar_datos = PythonOperator(
        task_id="filtrar_datos",
        python_callable=filtrar_datos_func,
        provide_context=True
    )

    top_ctr = PythonOperator(
        task_id="top_ctr",
        python_callable=top_ctr_func,
        provide_context=True
    )

    top_product = PythonOperator(
        task_id="top_product",
        python_callable=top_product_func,
        provide_context=True
    )

    db_writing = PythonOperator(
        task_id="db_writing",
        python_callable=insertar_en_postgres,
        provide_context=True
    )

    filtrar_datos >> [top_ctr, top_product] >> db_writing
