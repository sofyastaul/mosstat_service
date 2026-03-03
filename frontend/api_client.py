from __future__ import annotations
import requests

class ApiClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")

    def _url(self, path: str) -> str:
        return self.base_url + path

    def health(self):
        r = requests.get(self._url("/health"), timeout=20)
        r.raise_for_status()
        return r.json()

    def refresh(self):
        r = requests.post(self._url("/refresh"), timeout=300)
        r.raise_for_status()
        return r.json()

    def cpi_period(self, start_ym: str, end_ym: str):
        r = requests.get(self._url("/cpi/period"), params={"start_ym": start_ym, "end_ym": end_ym}, timeout=60)
        r.raise_for_status()
        return r.json()

    def cpi_year(self, year: int):
        r = requests.get(self._url(f"/cpi/year/{year}"), timeout=60)
        r.raise_for_status()
        return r.json()

    def income(self, indicator_contains: str | None, start_year: int | None, end_year: int | None):
        params = {}
        if indicator_contains:
            params["indicator_contains"] = indicator_contains
        if start_year is not None:
            params["start_year"] = start_year
        if end_year is not None:
            params["end_year"] = end_year
        r = requests.get(self._url("/income"), params=params, timeout=60)
        r.raise_for_status()
        return r.json()

    def poverty(self, start_year: int | None, end_year: int | None):
        params = {}
        if start_year is not None:
            params["start_year"] = start_year
        if end_year is not None:
            params["end_year"] = end_year
        r = requests.get(self._url("/poverty"), params=params, timeout=60)
        r.raise_for_status()
        return r.json()

    def morbidity(self, disease_contains: str | None, start_year: int | None, end_year: int | None):
        params = {}
        if disease_contains:
            params["disease_contains"] = disease_contains
        if start_year is not None:
            params["start_year"] = start_year
        if end_year is not None:
            params["end_year"] = end_year
        r = requests.get(self._url("/morbidity"), params=params, timeout=60)
        r.raise_for_status()
        return r.json()

    def medstaff(self, start_year: int | None, end_year: int | None):
        params = {}
        if start_year is not None:
            params["start_year"] = start_year
        if end_year is not None:
            params["end_year"] = end_year
        r = requests.get(self._url("/medstaff"), params=params, timeout=60)
        r.raise_for_status()
        return r.json()


    def income_indicators(self):
        r = requests.get(self._url("/income/indicators"), timeout=60)
        r.raise_for_status()
        return r.json()

    def morbidity_classes(self):
        r = requests.get(self._url("/morbidity/classes"), timeout=60)
        r.raise_for_status()
        return r.json()
