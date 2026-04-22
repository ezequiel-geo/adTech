## http://127.0.0.1:8000/docs

##cat > app/main.py <<'PY'
from fastapi import FastAPI, HTTPException
import pandas as pd
from pathlib import Path

app = FastAPI(title="TP Final Recommender API")

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_PATH = BASE_DIR / "data" / "processed" / "recommendations_ready.csv"

def load_data():
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"No existe el archivo: {DATA_PATH}")
    return pd.read_csv(DATA_PATH)

@app.get("/")
def home():
    return {"message": "API local funcionando"}

@app.get("/recommendations/{adv}/{modelo}")
def get_recommendations(adv: str, modelo: str):
    df = load_data()

    subset = df[
        (df["advertiser_id"].astype(str) == str(adv)) &
        (df["model_name"].astype(str) == str(modelo))
    ].sort_values("rank")

    if subset.empty:
        raise HTTPException(status_code=404, detail="No se encontraron recomendaciones")

    return {
        "advertiser_id": adv,
        "model_name": modelo,
        "count": len(subset),
        "recommendations": subset.to_dict(orient="records")
    }

@app.get("/history/{adv}")
def get_history(adv: str):
    df = load_data()

    subset = df[df["advertiser_id"].astype(str) == str(adv)].copy()

    if subset.empty:
        raise HTTPException(status_code=404, detail="No se encontró historial para ese advertiser")

    subset["run_date"] = pd.to_datetime(subset["run_date"])
    subset = subset.sort_values(["run_date", "model_name", "rank"], ascending=[False, True, True])

    # últimos 7 días disponibles para ese advertiser
    last_7_dates = subset["run_date"].dt.date.drop_duplicates().sort_values(ascending=False).head(7)
    subset = subset[subset["run_date"].dt.date.isin(last_7_dates)]

    history = []

    for run_date, df_date in subset.groupby(subset["run_date"].dt.date, sort=False):
        models = {}

        for model_name, df_model in df_date.groupby("model_name", sort=False):
            models[model_name] = df_model[
                ["rank", "product_id", "score"]
            ].sort_values("rank").to_dict(orient="records")

        history.append({
            "run_date": str(run_date),
            "models": models
        })

    return {
        "advertiser_id": adv,
        "days_count": len(history),
        "history": history
    }

@app.get("/stats/")
def get_stats():
    df = load_data()

    stats = {
        "rows_total": int(len(df)),
        "advertisers_total": int(df["advertiser_id"].nunique()),
        "models_total": int(df["model_name"].nunique()),
        "rows_by_model": df["model_name"].value_counts().to_dict(),
        "top_5_advertisers_by_rows": df["advertiser_id"].value_counts().head(5).to_dict()
    }

    return stats
##PY
