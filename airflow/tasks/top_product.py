import pandas as pd

BUCKET_NAME = "storage_bucket_groppo"
PROCESSED_DIR = f"gs://{BUCKET_NAME}/data/processed"

def run(ds):
    print("Leyendo product views...")
    products = pd.read_csv(f"{PROCESSED_DIR}/product_views_filtered.csv")

    print("Contando views...")
    views = products.groupby(["advertiser_id", "product_id"]).size().reset_index(name="score")

    print("Ranking productos...")
    views = views.sort_values(["advertiser_id", "score"], ascending=[True, False])
    views["rank"] = views.groupby("advertiser_id").cumcount() + 1
    views = views[views["rank"] <= 20].copy()

    views["model_name"] = "top_product"
    views["run_date"] = ds

    views = views[["run_date", "advertiser_id", "model_name", "rank", "product_id", "score"]]

    print("Guardando resultado en Storage...")
    views.to_csv(f"{PROCESSED_DIR}/top_product.csv", index=False)

    print("Top Product terminado")
