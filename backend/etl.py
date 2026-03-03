from __future__ import annotations

import re
from io import BytesIO
from urllib.parse import urljoin, urlsplit, urlunsplit, quote

import pandas as pd
import requests
import urllib3
from bs4 import BeautifulSoup

# ----------------------------
# HTTP / parsing helpers
# ----------------------------

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
            )
        }
    )
    return s

def safe_url(url: str) -> str:
    parts = urlsplit(url)
    safe_path = quote(parts.path, safe="/%")
    return urlunsplit((parts.scheme, parts.netloc, safe_path, parts.query, parts.fragment))

def fetch_text(session: requests.Session, url: str, verify_ssl: bool) -> str:
    r = session.get(url, timeout=30, verify=verify_ssl)
    r.raise_for_status()
    r.encoding = r.apparent_encoding
    return r.text

def download_bytes(session: requests.Session, url: str, verify_ssl: bool) -> bytes:
    r = session.get(url, timeout=60, verify=verify_ssl)
    r.raise_for_status()
    return r.content

def parse_documents_from_folder_html(html: str, base_url: str) -> pd.DataFrame:
    soup = BeautifulSoup(html, "lxml")
    rows = []
    for item in soup.select("div.document-list__item"):
        a = item.select_one("div.document-list__item-link a[href]")
        title_el = item.select_one("div.document-list__item-title")
        info_el = item.select_one("div.document-list__item-info")
        if not a or not title_el:
            continue

        href = a.get("href", "").strip()
        full_url = safe_url(urljoin(base_url, href))

        title = " ".join(title_el.get_text(" ", strip=True).split())
        info = info_el.get_text(" ", strip=True) if info_el else ""

        btn_text = a.get_text(" ", strip=True).upper()
        m_ext = re.search(r"\b(PDF|XLSX|XLS|CSV|DOCX|ZIP|WEB)\b", btn_text)
        ext = m_ext.group(1).lower() if m_ext else ""

        rows.append({"title": title, "url": full_url, "ext": ext, "info": info})

    return pd.DataFrame(rows).drop_duplicates(subset=["title", "url"]).reset_index(drop=True)

def get_folder_documents(session: requests.Session, folder_url: str, verify_ssl: bool) -> pd.DataFrame:
    html = fetch_text(session, folder_url, verify_ssl=verify_ssl)
    return parse_documents_from_folder_html(html, base_url=folder_url)

def find_xlsx_url(df_docs: pd.DataFrame, keywords: list[str]) -> str:
    tmp = df_docs.copy()
    tmp["title_norm"] = tmp["title"].astype(str).str.lower()
    mask = tmp["ext"].eq("xlsx")
    for kw in keywords:
        mask = mask & tmp["title_norm"].str.contains(str(kw).lower(), na=False)
    candidates = tmp[mask].copy()
    if candidates.empty:
        xlsx_all = tmp[tmp["ext"].eq("xlsx")][["title", "url"]]
        raise RuntimeError(
            "Не найден XLSX по ключевым словам.\n\n"
            f"Ключевые слова: {', '.join(keywords)}\n\n"
            "Все XLSX на странице:\n" + xlsx_all.to_string(index=False)
        )
    return candidates.iloc[0]["url"]

def cleanup_year_columns(cols):
    out = []
    for c in cols:
        s = str(c).strip()
        m = re.search(r"(\d{4})", s)
        out.append(int(m.group(1)) if m else s)
    return out

def normalize_strings(x):
    if pd.isna(x):
        return x
    return " ".join(str(x).replace("\n", " ").split())

def read_excel_bytes(xlsx_bytes: bytes, header):
    return pd.read_excel(BytesIO(xlsx_bytes), sheet_name=0, header=header, engine="openpyxl")

# ----------------------------
# Dataset parsers
# ----------------------------

MONTH_ORDER = [
    "Январь","Февраль","Март","Апрель","Май","Июнь",
    "Июль","Август","Сентябрь","Октябрь","Ноябрь","Декабрь"
]
MONTHS_LOWER = {m.lower() for m in MONTH_ORDER}
MONTH_TO_NUM = {m: i+1 for i,m in enumerate(MONTH_ORDER)}
NUM_TO_MONTH = {i+1: m for i,m in enumerate(MONTH_ORDER)}

def parse_cpi_df(xlsx_bytes: bytes) -> pd.DataFrame:
    df = read_excel_bytes(xlsx_bytes, header=3)
    df = df.rename(columns={df.columns[0]: "month"})
    df["month"] = df["month"].map(normalize_strings)
    df = df[df["month"].notna()].copy()

    df["_m"] = df["month"].astype(str).str.lower()
    df = df[df["_m"].isin(MONTHS_LOWER)].drop(columns=["_m"]).copy()

    year_cols = cleanup_year_columns(df.columns[1:])
    df.columns = ["month"] + year_cols

    out = df.melt(id_vars=["month"], var_name="year", value_name="cpi_index_prev_month")
    out = out[pd.to_numeric(out["year"], errors="coerce").notna()].copy()
    out["year"] = out["year"].astype(int)
    out["cpi_index_prev_month"] = pd.to_numeric(out["cpi_index_prev_month"], errors="coerce")

    out = out.drop_duplicates(subset=["year", "month"], keep="first")

    out["month"] = pd.Categorical(out["month"], categories=MONTH_ORDER, ordered=True)
    out = out.sort_values(["year", "month"]).reset_index(drop=True)
    out["month"] = out["month"].astype(str)

    return out

def parse_income_df(xlsx_bytes: bytes) -> pd.DataFrame:
    df = read_excel_bytes(xlsx_bytes, header=2)
    df = df.rename(columns={df.columns[0]: "indicator"})
    df["indicator"] = df["indicator"].map(normalize_strings)
    df = df[df["indicator"].notna()].copy()

    low = df["indicator"].astype(str).fillna("").str.lower()
    mask = (
        (~low.str.match(r"^\d+\)")) &
        (~low.str.contains("данные рассчитаны", na=False)) &
        (~low.str.contains("методолог", na=False))
    )
    df = df.loc[mask].copy()

    new_cols = ["indicator"] + cleanup_year_columns(df.columns[1:])
    df.columns = new_cols

    year_cols = [c for c in df.columns if isinstance(c, int)]
    for c in year_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    out = df.melt(id_vars=["indicator"], value_vars=year_cols, var_name="year", value_name="value")
    out["year"] = out["year"].astype(int)
    out = out.sort_values(["indicator", "year"]).reset_index(drop=True)
    return out

def parse_poverty_df(xlsx_bytes: bytes) -> pd.DataFrame:
    df = read_excel_bytes(xlsx_bytes, header=2)
    df.columns = ["year", "poverty_share_percent"]
    df = df[df["year"].notna()].copy()

    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["poverty_share_percent"] = pd.to_numeric(df["poverty_share_percent"], errors="coerce")

    df = df[df["year"].notna()].copy()
    df["year"] = df["year"].astype(int)
    df = df.sort_values("year").reset_index(drop=True)
    return df

def parse_morbidity_df(xlsx_bytes: bytes) -> pd.DataFrame:
    raw = pd.read_excel(BytesIO(xlsx_bytes), sheet_name=0, header=[3, 4], engine="openpyxl")

    disease = raw.iloc[:, 0].map(normalize_strings)
    disease_str = disease.astype(str).fillna("").str.lower()

    keep = disease.notna()
    keep = keep & (~disease_str.str.match(r"^\d+\)"))
    keep = keep & (~disease_str.str.contains("в расчете", na=False))

    disease = disease.loc[keep].reset_index(drop=True)
    data = raw.iloc[:, 1:].loc[keep].reset_index(drop=True)

    records = []
    for col in data.columns:
        year_part = normalize_strings(col[0])
        metric_part = normalize_strings(col[1])

        m = re.search(r"(\d{4})", str(year_part))
        if not m:
            continue
        year = int(m.group(1))

        metric = str(metric_part).lower()
        if "10" in metric and "тыс" in metric:
            metric_name = "per_10k"
        elif "1000" in metric or "тыс" in metric:
            metric_name = "per_1000"
        else:
            metric_name = "total"

        vals = pd.to_numeric(data[col], errors="coerce")

        records.append(
            pd.DataFrame(
                {"disease_class": disease, "year": year, "metric": metric_name, "value": vals}
            )
        )

    long_df = pd.concat(records, ignore_index=True)

    out = (
        long_df.pivot_table(
            index=["disease_class", "year"],
            columns="metric",
            values="value",
            aggfunc="first",
        )
        .reset_index()
    )

    out.columns.name = None
    rename_map = {}
    if "total" in out.columns:
        rename_map["total"] = "cases_total"
    # Не создаём столбцы cases_per_1000 и cases_per_10k: в исходных файлах они часто пустые
    out = out.rename(columns=rename_map)
    # На всякий случай удалим, если вдруг появились
    for col in ["cases_per_1000", "cases_per_10k", "per_1000", "per_10k"]:
        if col in out.columns:
            out = out.drop(columns=[col])


    out["year"] = out["year"].astype(int)
    out = out.sort_values(["disease_class", "year"]).reset_index(drop=True)
    return out

def parse_medstaff_df(xlsx_bytes: bytes) -> pd.DataFrame:
    df = pd.read_excel(BytesIO(xlsx_bytes), sheet_name=0, header=[2, 3], engine="openpyxl")

    flat_cols = []
    for a, b in df.columns:
        a = normalize_strings(a)
        b = normalize_strings(b)
        if a is None or str(a).startswith("Unnamed"):
            a = ""
        if b is None or str(b).startswith("Unnamed"):
            b = ""
        flat_cols.append((a + " " + b).strip())
    df.columns = flat_cols

    year_col = [c for c in df.columns if c.lower().startswith("годы")]
    if not year_col:
        raise RuntimeError("Не найдена колонка 'Годы' в файле медицинских кадров.")
    year_col = year_col[0]

    doctors_total_col = [c for c in df.columns if "численность врачей" in c.lower() and "всего" in c.lower()]
    doctors_rate_col = [c for c in df.columns if "численность врачей" in c.lower() and "10 000" in c.lower()]
    nurses_total_col = [c for c in df.columns if "среднего медицинского персонала" in c.lower() and "всего" in c.lower()]
    nurses_rate_col = [c for c in df.columns if "среднего медицинского персонала" in c.lower() and "10 000" in c.lower()]

    def pick_one(lst, label):
        if not lst:
            raise RuntimeError(f"Не найдена колонка: {label}")
        return lst[0]

    doctors_total_col = pick_one(doctors_total_col, "Численность врачей всего")
    doctors_rate_col = pick_one(doctors_rate_col, "Численность врачей на 10 000")
    nurses_total_col = pick_one(nurses_total_col, "Численность среднего персонала всего")
    nurses_rate_col = pick_one(nurses_rate_col, "Численность среднего персонала на 10 000")

    out = df[[year_col, doctors_total_col, doctors_rate_col, nurses_total_col, nurses_rate_col]].copy()
    out.columns = ["year", "doctors_total", "doctors_per_10k", "nurses_total", "nurses_per_10k"]

    out["year"] = pd.to_numeric(out["year"], errors="coerce")
    out = out[out["year"].notna()].copy()
    out["year"] = out["year"].astype(int)

    for c in ["doctors_total", "doctors_per_10k", "nurses_total", "nurses_per_10k"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    out = out.sort_values("year").reset_index(drop=True)
    return out

# ----------------------------
# Orchestrator
# ----------------------------

FOLDER_CPI = "https://77.rosstat.gov.ru/folder/64640"
FOLDER_LIVING = "https://77.rosstat.gov.ru/folder/64641"
FOLDER_HEALTH = "https://77.rosstat.gov.ru/folder/64643"

def load_all_mosstat_data(verify_ssl: bool=False) -> dict[str, pd.DataFrame]:
    if not verify_ssl:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    session = make_session()

    docs_cpi = get_folder_documents(session, FOLDER_CPI, verify_ssl=verify_ssl)
    docs_living = get_folder_documents(session, FOLDER_LIVING, verify_ssl=verify_ssl)
    docs_health = get_folder_documents(session, FOLDER_HEALTH, verify_ssl=verify_ssl)

    cpi_url = find_xlsx_url(docs_cpi, ["динамика", "индекса", "потребительских", "цен"])
    income_url = find_xlsx_url(docs_living, ["динамика", "денежных", "доходов", "москвы"])
    poverty_url = find_xlsx_url(docs_living, ["доля", "ниже", "прожиточного", "минимума"])
    morbidity_url = find_xlsx_url(docs_health, ["заболеваемость", "по", "основным", "классам", "болезней"])
    medstaff_url = find_xlsx_url(docs_health, ["численность", "медицинских", "кадров"])

    cpi_bytes = download_bytes(session, cpi_url, verify_ssl=verify_ssl)
    income_bytes = download_bytes(session, income_url, verify_ssl=verify_ssl)
    poverty_bytes = download_bytes(session, poverty_url, verify_ssl=verify_ssl)
    morbidity_bytes = download_bytes(session, morbidity_url, verify_ssl=verify_ssl)
    medstaff_bytes = download_bytes(session, medstaff_url, verify_ssl=verify_ssl)

    return {
        "cpi": parse_cpi_df(cpi_bytes),
        "income": parse_income_df(income_bytes),
        "poverty": parse_poverty_df(poverty_bytes),
        "morbidity": parse_morbidity_df(morbidity_bytes),
        "medstaff": parse_medstaff_df(medstaff_bytes),
        "docs_cpi": docs_cpi,
        "docs_living": docs_living,
        "docs_health": docs_health,
    }
