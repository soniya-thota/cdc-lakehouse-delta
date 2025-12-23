from typing import Optional
from fastapi import FastAPI, Query, HTTPException
from deltalake import DeltaTable
import pandas as pd

SILVER_PATH = "/data/delta/silver/orders"

app = FastAPI(
    title="Orders API (Delta Lake)",
    description="Reads Delta table snapshots (latest or by version) and returns JSON.",
    version="0.1.0",
)

def _load_dataframe(version: Optional[int] = None) -> pd.DataFrame:
    try:
        dt = DeltaTable(SILVER_PATH, version=version) if version is not None else DeltaTable(SILVER_PATH)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to open Delta table: {e}")
    try:
        df = dt.to_pandas()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read Delta data: {e}")
    if "_deleted" in df.columns:
        df = df[df["_deleted"] == False]  # noqa: E712
    return df

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/versions")
def list_versions(limit: int = Query(20, ge=1, le=100)):
    try:
        dt = DeltaTable(SILVER_PATH)
        latest = dt.version()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to open Delta table: {e}")
    start = max(0, latest - limit + 1)
    return {"latest": latest, "versions": list(range(start, latest + 1))}

@app.get("/orders")
def list_orders(
    version: Optional[int] = Query(default=None, ge=0),
    limit: int = Query(1000, ge=1, le=10000),
    offset: int = Query(0, ge=0),
):
    df = _load_dataframe(version)
    if "id" in df.columns:
        df = df.sort_values("id", kind="stable")
    return df.iloc[offset : offset + limit].to_dict(orient="records")

@app.get("/orders/{order_id}")
def get_order(order_id: int, version: Optional[int] = Query(default=None, ge=0)):
    df = _load_dataframe(version)
    if "id" not in df.columns:
        raise HTTPException(status_code=500, detail="Column 'id' not found in table.")
    row = df[df["id"] == order_id]
    if row.empty:
        raise HTTPException(status_code=404, detail=f"Order id={order_id} not found")
    return row.iloc[0].to_dict()
