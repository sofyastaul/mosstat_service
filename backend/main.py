from __future__ import annotations

import traceback
from fastapi import FastAPI, Query
from fastapi import HTTPException
from fastapi.responses import JSONResponse

from .config import settings
from .db import get_conn
from .service import refresh_all
from .repo import get_cpi, get_income, get_poverty, get_morbidity, get_medstaff, get_income_indicators, get_morbidity_classes
from .calculations import cpi_period_compound, cpi_year_compound

app = FastAPI(title="Mosstat Service", version="1.0.0")

@app.exception_handler(Exception)
def _all_exception_handler(request, exc: Exception):
    # Не ломаем стандартные HTTP-ошибки FastAPI
    if isinstance(exc, HTTPException):
        detail = exc.detail
        if isinstance(detail, dict):
            detail = detail.get("message") or str(detail)
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error_type": type(exc).__name__,
                "error_message": str(detail),
            },
        )
    # Человекочитаемые ошибки для пользователя
    if isinstance(exc, ValueError):
        return JSONResponse(
            status_code=422,
            content={
                "error_type": type(exc).__name__,
                "error_message": str(exc),
            },
        )
    tb = traceback.format_exc()
    return JSONResponse(
        status_code=500,
        content={
            "error_type": type(exc).__name__,
            "error_message": str(exc),
            "traceback": tb,
        },
    )

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/refresh")
def refresh():
    return refresh_all(verify_ssl=settings.verify_ssl)

# -----------------
# CPI (ИПЦ)
# -----------------

@app.get("/cpi/monthly")
def cpi_monthly(
    start_year: int | None = Query(default=None),
    end_year: int | None = Query(default=None),
):
    with get_conn() as conn:
        df = get_cpi(conn)
    if start_year is not None:
        df = df[df["year"] >= start_year]
    if end_year is not None:
        df = df[df["year"] <= end_year]
    return {"rows": df.to_dict(orient="records")}

@app.get("/cpi/period")
def cpi_period(
    start_ym: str = Query(..., description="YYYY-MM, например 2023-04"),
    end_ym: str = Query(..., description="YYYY-MM, например 2023-09"),
):
    with get_conn() as conn:
        df = get_cpi(conn)
    return cpi_period_compound(df, start_ym, end_ym)

@app.get("/cpi/year/{year}")
def cpi_year(year: int):
    with get_conn() as conn:
        df = get_cpi(conn)
    return cpi_year_compound(df, year)

# -----------------
# Income / Poverty / Morbidity / Medstaff
# -----------------

@app.get("/income/indicators")
def income_indicators():
    # До выполнения ETL таблицы могут отсутствовать — возвращаем пустой список без 500.
    try:
        with get_conn() as conn:
            df = get_income_indicators(conn)
        return {"indicators": df["indicator"].tolist()}
    except Exception:
        return {"indicators": []}

@app.get("/income")
def income(
    indicator_contains: str | None = Query(default=None, description="Поиск по названию показателя"),
    start_year: int | None = None,
    end_year: int | None = None,
):
    if start_year is not None and end_year is not None and start_year > end_year:
        raise ValueError("Начальный год не может быть больше конечного")
    with get_conn() as conn:
        df = get_income(conn, indicator_contains, start_year, end_year)
    if df.empty:
        raise HTTPException(status_code=404, detail="Выбранная дата отсутствует в БД")
    return {"rows": df.to_dict(orient="records")}

@app.get("/poverty")
def poverty(start_year: int | None = None, end_year: int | None = None):
    if start_year is not None and end_year is not None and start_year > end_year:
        raise ValueError("Начальный год не может быть больше конечного")
    with get_conn() as conn:
        df = get_poverty(conn, start_year, end_year)
    if df.empty:
        raise HTTPException(status_code=404, detail="Выбранная дата отсутствует в БД")
    return {"rows": df.to_dict(orient="records")}

@app.get("/morbidity/classes")
def morbidity_classes():
    # До выполнения ETL таблицы могут отсутствовать — возвращаем пустой список без 500.
    try:
        with get_conn() as conn:
            df = get_morbidity_classes(conn)
        return {"classes": df["disease_class"].tolist()}
    except Exception:
        return {"classes": []}

@app.get("/morbidity")
def morbidity(
    disease_contains: str | None = Query(default=None, description="Поиск по классу болезней"),
    start_year: int | None = None,
    end_year: int | None = None,
):
    if start_year is not None and end_year is not None and start_year > end_year:
        raise ValueError("Начальный год не может быть больше конечного")
    with get_conn() as conn:
        df = get_morbidity(conn, disease_contains, start_year, end_year)
    if df.empty:
        raise HTTPException(status_code=404, detail="Выбранная дата отсутствует в БД")
    return {"rows": df.to_dict(orient="records")}

@app.get("/medstaff")
def medstaff(start_year: int | None = None, end_year: int | None = None):
    if start_year is not None and end_year is not None and start_year > end_year:
        raise ValueError("Начальный год не может быть больше конечного")
    with get_conn() as conn:
        df = get_medstaff(conn, start_year, end_year)
    if df.empty:
        raise HTTPException(status_code=404, detail="Выбранная дата отсутствует в БД")
    return {"rows": df.to_dict(orient="records")}
