from __future__ import annotations
import traceback
from datetime import datetime

from .db import get_conn
from .schema import DDL
from .upsert import upsert_df
from .etl import load_all_mosstat_data

def ensure_tables(conn):
    with conn.cursor() as cur:
        cur.execute(DDL)
        # На случай если таблица была создана раньше со лишними столбцами
        cur.execute("alter table if exists mosstat_morbidity drop column if exists cases_per_1000")
        cur.execute("alter table if exists mosstat_morbidity drop column if exists cases_per_10k")
    conn.commit()

def refresh_all(verify_ssl: bool=False) -> dict:
    """Скачивает, парсит, и загружает в PostgreSQL (UPSERT)."""
    data = load_all_mosstat_data(verify_ssl=verify_ssl)
    cpi, income, poverty, morbidity, medstaff = (
        data["cpi"], data["income"], data["poverty"], data["morbidity"], data["medstaff"]
    )

    with get_conn() as conn:
        ensure_tables(conn)

        upsert_df(
            conn,
            cpi,
            "mosstat_cpi",
            cols=["year", "month", "cpi_index_prev_month"],
            conflict_cols=["year", "month"],
            update_cols=["cpi_index_prev_month"],
        )

        upsert_df(
            conn,
            income,
            "mosstat_income",
            cols=["indicator", "year", "value"],
            conflict_cols=["indicator", "year"],
            update_cols=["value"],
        )

        upsert_df(
            conn,
            poverty,
            "mosstat_poverty",
            cols=["year", "poverty_share_percent"],
            conflict_cols=["year"],
            update_cols=["poverty_share_percent"],
        )

        morb_cols = ["disease_class", "year"]
        if "cases_total" in morbidity.columns:
            morb_cols.append("cases_total")

        upsert_df(
            conn,
            morbidity,
            "mosstat_morbidity",
            cols=morb_cols,
            conflict_cols=["disease_class", "year"],
            update_cols=[c for c in morb_cols if c not in ["disease_class", "year"]],
        )

        upsert_df(
            conn,
            medstaff,
            "mosstat_medstaff",
            cols=["year", "doctors_total", "doctors_per_10k", "nurses_total", "nurses_per_10k"],
            conflict_cols=["year"],
            update_cols=["doctors_total", "doctors_per_10k", "nurses_total", "nurses_per_10k"],
        )

    return {
        "status": "ok",
        "refreshed_at": datetime.utcnow().isoformat() + "Z",
        "rows": {
            "cpi": int(cpi.shape[0]),
            "income": int(income.shape[0]),
            "poverty": int(poverty.shape[0]),
            "morbidity": int(morbidity.shape[0]),
            "medstaff": int(medstaff.shape[0]),
        },
        "sources": {
            "cpi_folder": "https://77.rosstat.gov.ru/folder/64640",
            "living_folder": "https://77.rosstat.gov.ru/folder/64641",
            "health_folder": "https://77.rosstat.gov.ru/folder/64643",
        },
    }
