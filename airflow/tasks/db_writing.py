import os
import pandas as pd

PROCESSED_DIR = os.path.join(os.environ["AIRFLOW_HOME"], "..", "data", "processed")


def run():
    print("📥 Leyendo archivos finales...")

    ctr = pd.read_csv(os.path.join(PROCESSED_DIR, "top_ctr.csv"))
    prod = pd.read_csv(os.path.join(PROCESSED_DIR, "top_product.csv"))

    print("🔗 Uniendo resultados...")

    final_df = pd.concat([ctr, prod], ignore_index=True)

    final_df = final_df.sort_values(
        ["run_date", "advertiser_id", "model_name", "rank"]
    ).reset_index(drop=True)

    print("🧪 Validando columnas...")

    expected_cols = ["run_date", "advertiser_id", "model_name", "rank", "product_id", "score"]
    if list(final_df.columns) != expected_cols:
        raise ValueError(f"Columnas inesperadas: {list(final_df.columns)}")

    print("💾 Guardando consolidado local...")

    final_df.to_csv(os.path.join(PROCESSED_DIR, "recommendations_ready.csv"), index=False)

    print("✅ DBWriting local terminado")
    print(f"📊 Filas totales: {len(final_df)}")
    print(f"📌 Advertisers únicos: {final_df['advertiser_id'].nunique()}")
    print("📋 Modelos presentes:")
    print(final_df["model_name"].value_counts())


if __name__ == "__main__":
    run()
