import pandas as pd

BUCKET_NAME = "storage_bucket_groppo"
DATA_DIR = f"gs://{BUCKET_NAME}/data"
PROCESSED_DIR = f"{DATA_DIR}/processed"

def run(ds):
    print(f"Leyendo datos desde Cloud Storage para la fecha {ds}...")
    ads = pd.read_csv(f"{DATA_DIR}/ads_views.csv")
    products = pd.read_csv(f"{DATA_DIR}/product_views.csv")
    active = pd.read_csv(f"{DATA_DIR}/advertiser_ids.csv")

    print("Filtrando logs del día...")
    ads = ads[ads["date"] == ds]
    products = products[products["date"] == ds]

    print("Filtrando advertisers activos...")
    ads = ads[ads["advertiser_id"].isin(active["advertiser_id"])]
    products = products[products["advertiser_id"].isin(active["advertiser_id"])]

    print("Guardando resultados en Cloud Storage...")
    ads.to_csv(f"{PROCESSED_DIR}/ads_views_filtered.csv", index=False)
    products.to_csv(f"{PROCESSED_DIR}/product_views_filtered.csv", index=False)

    print("Filter terminado")
