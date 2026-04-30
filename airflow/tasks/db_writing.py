import pandas as pd
from sqlalchemy import create_engine
from airflow.models import Variable

BUCKET_NAME = "storage_bucket_groppo"
PROCESSED_DIR = f"gs://{BUCKET_NAME}/data/processed"

DB_USER = Variable.get("DB_USER")
DB_PASS = Variable.get("DB_PASS")
DB_HOST = Variable.get("DB_HOST")
DB_NAME = Variable.get("DB_NAME")
DB_PORT = "5432"

def run():
    print("Leyendo archivos finales desde Storage...")
    ctr = pd.read_csv(f"{PROCESSED_DIR}/top_ctr.csv")
    prod = pd.read_csv(f"{PROCESSED_DIR}/top_product.csv")

    print("Uniendo resultados...")
    final_df = pd.concat([ctr, prod], ignore_index=True)

    final_df = final_df.sort_values(
        ["run_date", "advertiser_id", "model_name", "rank"]
    ).reset_index(drop=True)

    print("Validando columnas...")
    expected_cols = ["run_date", "advertiser_id", "model_name", "rank", "product_id", "score"]
    if list(final_df.columns) != expected_cols:
        raise ValueError(f"Columnas inesperadas: {list(final_df.columns)}")

    print("Guardando consolidado en Storage...")
    final_df.to_csv(f"{PROCESSED_DIR}/recommendations_ready.csv", index=False)

    print("Escribiendo en Cloud SQL (modo append para historial)...")
    engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    
    
    final_df.to_sql("recommendations", engine, if_exists="append", index=False)

    print("DBWriting terminado")
    print(f"Filas totales agregadas: {len(final_df)}")
