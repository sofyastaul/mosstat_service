from __future__ import annotations

import datetime as dt
import math
import pandas as pd

from .etl import MONTH_TO_NUM, NUM_TO_MONTH


def parse_ym(s: str) -> tuple[int, int]:
    """Парсит 'YYYY-MM' и валидирует месяц."""
    try:
        y_str, m_str = s.strip().split("-")
        y = int(y_str)
        m = int(m_str)
        if m < 1 or m > 12:
            raise ValueError
        # дополнительно проверим, что дата вообще создаётся
        dt.date(y, m, 1)
        return y, m
    except Exception:
        raise ValueError(
            "Некорректная дата. Используйте формат YYYY-MM и месяц 01-12 (например 2023-04)."
        )


def month_seq(start_ym: str, end_ym: str) -> list[tuple[int, int]]:
    sy, sm = parse_ym(start_ym)
    ey, em = parse_ym(end_ym)
    if (ey, em) < (sy, sm):
        raise ValueError("Конец периода должен быть не раньше начала")

    cur = dt.date(sy, sm, 1)
    end = dt.date(ey, em, 1)
    out: list[tuple[int, int]] = []
    while cur <= end:
        out.append((cur.year, cur.month))
        # add month
        if cur.month == 12:
            cur = dt.date(cur.year + 1, 1, 1)
        else:
            cur = dt.date(cur.year, cur.month + 1, 1)
    return out


def _compound_index(vals: list[float]) -> float:
    """Геометрическое перемножение (v/100) с логарифмами, чтобы избегать переполнения."""
    if not vals:
        raise ValueError("Выбранная дата отсутствует в БД")
    s = 0.0
    for v in vals:
        if v is None or pd.isna(v) or v <= 0:
            raise ValueError("Некорректные значения ИПЦ в БД")
        s += math.log(float(v) / 100.0)
    return math.exp(s) * 100.0


def cpi_period_compound(cpi_df: pd.DataFrame, start_ym: str, end_ym: str) -> dict:
    """Считает ИПЦ за период как:
    product(index_month_prev) / 100^n * 100

    Реализация идёт через логарифмы, чтобы не падать на длинных периодах.
    """
    seq = month_seq(start_ym, end_ym)
    need = [
        {"year": y, "month": NUM_TO_MONTH[m], "ym": f"{y:04d}-{m:02d}"}
        for y, m in seq
    ]
    need_df = pd.DataFrame(need)

    df = cpi_df.merge(need_df, on=["year", "month"], how="right")
    missing = df[df["cpi_index_prev_month"].isna()][["ym"]]
    if not missing.empty:
        raise ValueError("Выбранная дата отсутствует в БД")

    vals = df["cpi_index_prev_month"].astype(float).tolist()
    n = len(vals)

    result = _compound_index(vals)

    formula = " * ".join([f"{v:.2f}" for v in vals]) + f" / 100^{n} * 100"

    return {
        "start": start_ym,
        "end": end_ym,
        "months": df[["ym", "cpi_index_prev_month"]].to_dict(orient="records"),
        "n_months": n,
        "compound_index_percent": round(float(result), 2),
        "formula": formula,
    }


def cpi_year_compound(cpi_df: pd.DataFrame, year: int) -> dict:
    df = cpi_df[cpi_df["year"] == year].copy()
    if df.empty:
        raise ValueError("Выбранная дата отсутствует в БД")

    df["mnum"] = df["month"].map(MONTH_TO_NUM)
    df = df.sort_values("mnum")

    vals = df["cpi_index_prev_month"].astype(float).tolist()
    n = len(vals)

    result = _compound_index(vals)
    formula = " * ".join([f"{v:.2f}" for v in vals]) + f" / 100^{n} * 100"

    return {
        "year": year,
        "months_available": df[["month", "cpi_index_prev_month"]].to_dict(orient="records"),
        "n_months": n,
        "compound_index_percent": round(float(result), 2),
        "formula": formula,
    }
