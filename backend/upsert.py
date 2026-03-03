from __future__ import annotations
import pandas as pd
from psycopg2.extras import execute_values

def _conv(v):
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    if hasattr(v, "item") and callable(v.item):
        try:
            return v.item()
        except Exception:
            return v
    return v

def upsert_df(conn, df: pd.DataFrame, table_name: str, cols: list[str],
              conflict_cols: list[str], update_cols: list[str] | None):
    df2 = df[cols].copy()
    rows = [tuple(_conv(v) for v in row) for row in df2.itertuples(index=False, name=None)]

    col_list = ", ".join(cols)
    conflict_list = ", ".join(conflict_cols)

    if update_cols:
        set_part = ", ".join([f"{c}=excluded.{c}" for c in update_cols])
        on_conflict = f"on conflict ({conflict_list}) do update set {set_part}"
    else:
        on_conflict = f"on conflict ({conflict_list}) do nothing"

    sql = f"insert into {table_name} ({col_list}) values %s {on_conflict};"
    with conn.cursor() as cur:
        execute_values(cur, sql, rows, page_size=5000)
    conn.commit()
