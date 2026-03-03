from __future__ import annotations
import pandas as pd

def q_df(conn, sql: str, params: tuple = ()) -> pd.DataFrame:
    return pd.read_sql_query(sql, conn, params=params)

def get_cpi(conn) -> pd.DataFrame:
    return q_df(conn, "select year, month, cpi_index_prev_month from mosstat_cpi")

def get_income(conn, indicator_contains: str | None, start_year: int | None, end_year: int | None) -> pd.DataFrame:
    where = []
    params = []
    if indicator_contains:
        where.append("lower(indicator) like %s")
        params.append(f"%{indicator_contains.lower()}%")
    if start_year is not None:
        where.append("year >= %s")
        params.append(start_year)
    if end_year is not None:
        where.append("year <= %s")
        params.append(end_year)
    w = (" where " + " and ".join(where)) if where else ""
    return q_df(conn, f"select indicator, year, value from mosstat_income{w} order by indicator, year", tuple(params))

def get_poverty(conn, start_year: int | None, end_year: int | None) -> pd.DataFrame:
    where = []
    params = []
    if start_year is not None:
        where.append("year >= %s")
        params.append(start_year)
    if end_year is not None:
        where.append("year <= %s")
        params.append(end_year)
    w = (" where " + " and ".join(where)) if where else ""
    return q_df(conn, f"select year, poverty_share_percent from mosstat_poverty{w} order by year", tuple(params))

def get_morbidity(conn, disease_contains: str | None, start_year: int | None, end_year: int | None) -> pd.DataFrame:
    where = []
    params = []
    if disease_contains:
        where.append("lower(disease_class) like %s")
        params.append(f"%{disease_contains.lower()}%")
    if start_year is not None:
        where.append("year >= %s")
        params.append(start_year)
    if end_year is not None:
        where.append("year <= %s")
        params.append(end_year)
    w = (" where " + " and ".join(where)) if where else ""
    # В сервисе используем только итоговое значение cases_total.
    # (колонки cases_per_1000/cases_per_10k не создаём и, если были, удаляем в ensure_tables)
    return q_df(conn, f"""
        select disease_class, year, cases_total
        from mosstat_morbidity{w}
        order by disease_class, year
    """, tuple(params))

def get_medstaff(conn, start_year: int | None, end_year: int | None) -> pd.DataFrame:
    where = []
    params = []
    if start_year is not None:
        where.append("year >= %s")
        params.append(start_year)
    if end_year is not None:
        where.append("year <= %s")
        params.append(end_year)
    w = (" where " + " and ".join(where)) if where else ""
    return q_df(conn, f"""
        select year, doctors_total, doctors_per_10k, nurses_total, nurses_per_10k
        from mosstat_medstaff{w}
        order by year
    """, tuple(params))


def get_income_indicators(conn) -> pd.DataFrame:
    return q_df(conn, "select distinct indicator from mosstat_income order by indicator")

def get_morbidity_classes(conn) -> pd.DataFrame:
    return q_df(conn, "select distinct disease_class from mosstat_morbidity order by disease_class")
