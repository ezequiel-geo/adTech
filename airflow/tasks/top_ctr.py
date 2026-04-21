import os
import pandas as pd
from datetime import date

PROCESSED_DIR = os.path.join(os.environ["AIRFLOW_HOME"], "..", "data", "processed")


def run():
    print("📥 Leyendo datos filtrados...")

    ads = pd.read_csv(os.path.join(PROCESSED_DIR, "ads_views_filtered.csv"))

    print("📊 Calculando CTR...")

    ctr = ads.groupby(["advertiser_id", "product_id", "type"]).size().unstack(fill_value=0)

    if "click" not in ctr.columns:
        ctr["click"] = 0
    if "impression" not in ctr.columns:
        ctr["impression"] = 1

    ctr["score"] = ctr["click"] / ctr["impression"]
    ctr = ctr.reset_index()

    print("🏆 Generando ranking...")

    ctr = ctr.sort_values(["advertiser_id", "score"], ascending=[True, False])
    ctr["rank"] = ctr.groupby("advertiser_id").cumcount() + 1
    ctr = ctr[ctr["rank"] <= 20].copy()

    ctr["model_name"] = "top_ctr"
    ctr["run_date"] = date.today().isoformat()

    ctr = ctr[["run_date", "advertiser_id", "model_name", "rank", "product_id", "score"]]

    print("💾 Guardando resultados...")

    ctr.to_csv(os.path.join(PROCESSED_DIR, "top_ctr.csv"), index=False)

    print("✅ Top CTR terminado")


if __name__ == "__main__":
    run()
