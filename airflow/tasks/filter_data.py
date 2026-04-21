import os
import pandas as pd

DATA_DIR = os.path.join(os.environ["AIRFLOW_HOME"], "..", "data")
PROCESSED_DIR = os.path.join(DATA_DIR, "processed")


def run():
    print("📥 Leyendo datos...")

    ads = pd.read_csv(os.path.join(DATA_DIR, "ads_views"))
    products = pd.read_csv(os.path.join(DATA_DIR, "product_views"))
    active = pd.read_csv(os.path.join(DATA_DIR, "advertiser_ids"))

    print("🔍 Filtrando advertisers activos...")

    ads = ads[ads["advertiser_id"].isin(active["advertiser_id"])]
    products = products[products["advertiser_id"].isin(active["advertiser_id"])]

    print("💾 Guardando resultados...")

    os.makedirs(PROCESSED_DIR, exist_ok=True)
    ads.to_csv(os.path.join(PROCESSED_DIR, "ads_views_filtered.csv"), index=False)
    products.to_csv(os.path.join(PROCESSED_DIR, "product_views_filtered.csv"), index=False)

    print("✅ Filter terminado")


if __name__ == "__main__":
    run()
