# api/app.py
from typing import Optional, List
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
    """
    Load the Delta table as a pandas DataFrame.
    If version is provided, time-travel to that version.
    Filters out rows marked as logically deleted (_deleted = True).
    """
    try:
        dt = DeltaTable(SILVER_PATH, version=version) if version is not None else DeltaTable(SILVER_PATH)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to open Delta table: {e}")

    try:
        df = dt.to_pandas()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read Delta data: {e}")

    # If the column exists, hide tombstoned rows
    if "_deleted" in df.columns:
        df = df[df["_deleted"] == False]  # noqa: E712 (intentional comparison to False)

    return df

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/versions", summary="List available Delta versions (best-effort)")
def list_versions(limit: int = Query(20, ge=1, le=100)):
    """
    Return the latest version number and (best-effort) last N versions.
    Note: deltalake Python API provides latest_version; historical listing is approximated.
    """
    try:
        dt = DeltaTable(SILVER_PATH)
        latest = dt.version()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to open Delta table: {e}")

    # We can't enumerate all versions cheaply; return a window [max(0, latest-limit+1) .. latest]
    start = max(0, latest - limit + 1)
    return {"latest": latest, "versions": list(range(start, latest + 1))}

@app.get("/orders", summary="List orders (latest snapshot or by version)")
def list_orders(
    version: Optional[int] = Query(default=None, ge=0, description="Delta table version for time travel"),
    limit: int = Query(1000, ge=1, le=10000),
    offset: int = Query(0, ge=0),
):
    df = _load_dataframe(version)
    # Basic pagination
    df_page = df.sort_values("id", kind="stable").iloc[offset : offset + limit]
    return df_page.to_dict(orient="records")

@app.get("/orders/{order_id}", summary="Get a single order by id")
def get_order(
    order_id: int,
    version: Optional[int] = Query(default=None, ge=0, description="Delta table version for time travel"),
):
    df = _load_dataframe(version)
    if "id" not in df.columns:
        raise HTTPException(status_code=500, detail="Column 'id' not found in table.")
    row = df[df["id"] == order_id]
    if row.empty:
        raise HTTPException(status_code=404, detail=f"Order id={order_id} not found")
    return row.iloc[0].to_dict()

