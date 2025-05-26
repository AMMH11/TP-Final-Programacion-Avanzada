from fastapi import FastAPI
from datetime import date, timedelta
from app.db import get_connection

app = FastAPI()

@app.get("/recommendations/{adv_id}/{model}")
def get_recommendations(adv_id: str, model: str):
    model = model.lower()
    fecha_filtrada = (date.today() - timedelta(days=1)).isoformat()

    if model == "top_ctr":
        query = """
            SELECT product_id
            FROM top_ctr
            WHERE advertiser_id = %s AND fecha::date = %s::date
            GROUP BY product_id
            ORDER BY COUNT(*) DESC
            LIMIT 20
        """
    elif model == "top_product":
        query = """
            SELECT product_id
            FROM top_product
            WHERE advertiser_id = %s AND fecha::date = %s::date
            ORDER BY views DESC
            LIMIT 20
        """
    else:
        return {"error": "Modelo inválido. Usa 'top_ctr' o 'top_product'."}

    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(query, (adv_id, fecha_filtrada))
    results = cursor.fetchall()
    cursor.close()
    conn.close()

    return {
        "advertiser_id": adv_id,
        "model": model,
        "products": [r[0] for r in results]
    }

@app.get("/history/{adv_id}")
def get_history(adv_id: str):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT product_id, fecha FROM top_ctr
        WHERE advertiser_id = %s AND fecha::date >= CURRENT_DATE - interval '7 days'
        ORDER BY fecha DESC
    """, (adv_id,))
    ctr = cursor.fetchall()

    cursor.execute("""
        SELECT product_id, fecha FROM top_product
        WHERE advertiser_id = %s AND fecha::date >= CURRENT_DATE - interval '7 days'
        ORDER BY fecha DESC
    """, (adv_id,))
    prod = cursor.fetchall()

    cursor.close()
    conn.close()
    return {
        "advertiser_id": adv_id,
        "top_ctr_history": ctr,
        "top_product_history": prod
    }

@app.get("/stats/")
def get_stats():
    conn = get_connection()
    cursor = conn.cursor()

    # Advertisers únicos (union de ambas tablas)
    cursor.execute("""
        SELECT COUNT(DISTINCT advertiser_id) FROM (
            SELECT advertiser_id FROM top_ctr
            UNION
            SELECT advertiser_id FROM top_product
        ) AS all_advertisers
    """)
    advertisers_count = cursor.fetchone()[0]

    # Productos únicos (union de ambas tablas)
    cursor.execute("""
        SELECT COUNT(DISTINCT product_id) FROM (
            SELECT product_id FROM top_ctr
            UNION
            SELECT product_id FROM top_product
        ) AS all_products
    """)
    products_count = cursor.fetchone()[0]

    # Total recomendaciones (suma de ambas tablas)
    cursor.execute("SELECT COUNT(*) FROM top_ctr")
    ctr_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM top_product")
    product_count = cursor.fetchone()[0]

    recommendations_count = ctr_count + product_count

    # Última fecha registrada
    cursor.execute("""
        SELECT MAX(fecha) FROM (
            SELECT fecha FROM top_ctr
            UNION
            SELECT fecha FROM top_product
        ) AS all_dates
    """)
    last_update = cursor.fetchone()[0]

    # Modelo más usado
    most_used_model = "top_ctr" if ctr_count >= product_count else "top_product"

    cursor.close()
    conn.close()

    return {
        "advertisers_count": advertisers_count,
        "products_count": products_count,
        "recommendations_count": recommendations_count,
        "last_update": last_update.isoformat() if last_update else None,
        "most_used_model": most_used_model
    }

