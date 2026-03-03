from __future__ import annotations

import os
import sys

sys.path.append(os.path.dirname(__file__))

import streamlit as st
import pandas as pd
import requests
from api_client import ApiClient

st.set_page_config(page_title="Мосстат сервис", page_icon="📊", layout="wide")

st.title("📊 Mosstat Service")


def show_api_error(e: requests.HTTPError):
    """Показывает аккуратное сообщение ошибки от API (без сырого JSON)."""
    try:
        payload = e.response.json()
        msg = payload.get("error_message") or payload.get("detail") or str(e)
        st.error(msg)
    except Exception:
        st.error(str(e))

with st.sidebar:
    st.header("Подключение к API")
    base_url = st.text_input("Base URL", value="http://127.0.0.1:8000")
    api = ApiClient(base_url)

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Проверить API"):
            try:
                st.success(api.health())
            except Exception as e:
                st.error(f"Ошибка: {e}")

    with col2:
        if st.button("Обновить данные (ETL)"):
            try:
                with st.spinner("Скачиваю, парсю и загружаю в PostgreSQL..."):
                    res = api.refresh()
                st.success("Готово")
                st.json(res)
                # После обновления нужно заново подтянуть варианты показателей из БД
                st.cache_data.clear()
                st.rerun()
            except requests.HTTPError as e:
                show_api_error(e)
            except Exception as e:
                st.error(str(e))

@st.cache_data(show_spinner=False)
def cached_income_indicators(base_url: str):
    """Возвращает список показателей из БД.
    До загрузки данных таблицы могут отсутствовать — в этом случае возвращаем пустой список без падения приложения.
    """
    try:
        return ApiClient(base_url).income_indicators().get("indicators", [])
    except requests.RequestException:
        return []

@st.cache_data(show_spinner=False)
def cached_morbidity_classes(base_url: str):
    """Возвращает список классов болезней из БД.
    До загрузки данных таблицы могут отсутствовать — в этом случае возвращаем пустой список без падения приложения.
    """
    try:
        return ApiClient(base_url).morbidity_classes().get("classes", [])
    except requests.RequestException:
        return []

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "1) ИПЦ (период/год)",
    "2) Доходы населения",
    "3) Уровень бедности",
    "4) Заболеваемость",
    "5) Медицинские кадры",
])

# 1) CPI
with tab1:
    st.subheader("1) Динамика ИПЦ на товары и услуги (месяц к предыдущему)")
    st.write("Расчёт за период: произведение индексов / 100^n * 100.")
    c1, c2 = st.columns(2)
    with c1:
        start_ym = st.text_input("Начало периода (YYYY-MM)", value="2023-04")
    with c2:
        end_ym = st.text_input("Конец периода (YYYY-MM)", value="2023-09")

    if st.button("Посчитать ИПЦ за период"):
        try:
            res = api.cpi_period(start_ym, end_ym)
            st.metric("ИПЦ за период, %", res["compound_index_percent"])
            st.code(res["formula"])
            st.dataframe(pd.DataFrame(res["months"]), use_container_width=True)
        except requests.HTTPError as e:
            show_api_error(e)
        except Exception as e:
            st.error(str(e))

    st.divider()
    year = st.number_input("ИПЦ за весь год (введите год)", min_value=1990, max_value=2100, value=2024, step=1)
    if st.button("Посчитать ИПЦ за год"):
        try:
            res = api.cpi_year(int(year))
            st.metric(f"ИПЦ за {int(year)} год, %", res["compound_index_percent"])
            st.code(res["formula"])
            st.dataframe(pd.DataFrame(res["months_available"]), use_container_width=True)
        except requests.HTTPError as e:
            show_api_error(e)
        except Exception as e:
            st.error(str(e))

# 2) Income
with tab2:
    st.subheader("2) Динамика денежных доходов населения Москвы")
    indicators = cached_income_indicators(base_url)
    if not indicators:
        st.info("Списка показателей пока нет. Нажми \"Обновить данные (ETL)\" слева.")
        q = ""
    else:
        q = st.selectbox("Показатель", options=indicators, index=0)
    c1, c2 = st.columns(2)
    with c1:
        sy = st.number_input("Start year", min_value=1990, max_value=2100, value=2014, step=1)
    with c2:
        ey = st.number_input("End year", min_value=1990, max_value=2100, value=2024, step=1)

    if st.button("Показать доходы", disabled=(q == "")):
        try:
            rows = api.income(q, int(sy), int(ey))["rows"]
            df = pd.DataFrame(rows)
            st.dataframe(df, use_container_width=True)
        except requests.HTTPError as e:
            show_api_error(e)
        except Exception as e:
            st.error(str(e))

# 3) Poverty
with tab3:
    st.subheader("3) Уровень бедности по Москве")
    c1, c2 = st.columns(2)
    with c1:
        sy = st.number_input("Start year (бедность)", min_value=1990, max_value=2100, value=2014, step=1, key="p1")
    with c2:
        ey = st.number_input("End year (бедность)", min_value=1990, max_value=2100, value=2024, step=1, key="p2")

    if st.button("Показать уровень бедности"):
        try:
            rows = api.poverty(int(sy), int(ey))["rows"]
            st.dataframe(pd.DataFrame(rows), use_container_width=True)
        except requests.HTTPError as e:
            show_api_error(e)
        except Exception as e:
            st.error(str(e))

# 4) Morbidity
with tab4:
    st.subheader("4) Заболеваемость по классам болезней (Москва)")
    classes = cached_morbidity_classes(base_url)
    if not classes:
        st.info("Списка классов болезней пока нет. Нажми \"Обновить данные (ETL)\" слева.")
        q = ""
    else:
        q = st.selectbox("Класс болезни", options=classes, index=0)
    c1, c2 = st.columns(2)
    with c1:
        sy = st.number_input("Start year (заболеваемость)", min_value=1990, max_value=2100, value=2015, step=1, key="m1")
    with c2:
        ey = st.number_input("End year (заболеваемость)", min_value=1990, max_value=2100, value=2024, step=1, key="m2")

    if st.button("Показать заболеваемость", disabled=(q == "")):
        try:
            rows = api.morbidity(q, int(sy), int(ey))["rows"]
            st.dataframe(pd.DataFrame(rows), use_container_width=True)
        except requests.HTTPError as e:
            show_api_error(e)
        except Exception as e:
            st.error(str(e))

# 5) Medstaff
with tab5:
    st.subheader("5) Численность медицинских кадров (Москва)")
    c1, c2 = st.columns(2)
    with c1:
        sy = st.number_input("Start year (кадры)", min_value=1990, max_value=2100, value=2015, step=1, key="h1")
    with c2:
        ey = st.number_input("End year (кадры)", min_value=1990, max_value=2100, value=2024, step=1, key="h2")

    if st.button("Показать медицинские кадры"):
        try:
            rows = api.medstaff(int(sy), int(ey))["rows"]
            st.dataframe(pd.DataFrame(rows), use_container_width=True)
        except requests.HTTPError as e:
            show_api_error(e)
        except Exception as e:
            st.error(str(e))
