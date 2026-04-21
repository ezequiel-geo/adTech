import os
import pandas as pd
from datetime import date

PROCESSED_DIR = os.path.join(os.environ["AIRFLOW_HOME"], "..", "data", "processed")


def run():
    print("📥 Leyendo product views...")

    products = pd.read_csv(os.path.join(PROCESSED_DIR, "product_views_filtered.csv"))

    print("📊 Contando views...")

    views = products.groupby(["advertiser_id", "product_id"]).size().reset_index(name="score")

    print("🏆 Ranking productos...")

    views = views.sort_values(["advertiser_id", "score"], ascending=[True, False])
    views["rank"] = views.groupby("advertiser_id").cumcount() + 1
    views = views[views["rank"] <= 20].copy()

    views["model_name"] = "top_product"
    views["run_date"] = date.today().isoformat()

    views = views[["run_date", "advertiser_id", "model_name", "rank", "product_id", "score"]]

    print("💾 Guardando resultado...")

    views.to_csv(os.path.join(PROCESSED_DIR, "top_product.csv"), index=False)

    print("✅ Top Product terminado")


if __name__ == "__main__":
    run()
