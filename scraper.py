"""
Dedicated market data collection for the Excel report.

Sources:
- CO2 history from Investing
- BASE yearly contracts from TGE
- Electricity SPOT history from PSE API (hourly aggregation from quarter-hour prices)
- Gas spot index from TGE
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta
from io import StringIO
from typing import Iterable

import pandas as pd
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

DEFAULT_SOURCES = {
    "energy_base": "https://tge.pl/energia-elektryczna-otf",
    "power_spot": "https://api.raporty.pse.pl/api/csdac-pln",
    "gas_spot": "https://tge.pl/gaz-rdn",
    "co2_history": "https://pl.investing.com/etfs/co2-historical-data?utm_source=chatgpt.com",
}

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,*/*;q=0.8"
    ),
}

API_HEADERS = {
    "User-Agent": REQUEST_HEADERS["User-Agent"],
    "Accept": "application/json",
}

TGE_HISTORY_DAYS = 30


def _normalize_label(value: object) -> str:
    text = str(value or "").strip().lower()
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text)
    return text


def _to_float(value: object) -> float | None:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None

    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "-", "--"}:
        return None

    text = text.replace("\xa0", "").replace(" ", "")
    text = text.replace("%", "")
    text = text.replace(",", ".")
    text = re.sub(r"[^0-9.\-]", "", text)

    if not text or text in {"-", ".", "-."}:
        return None

    try:
        return float(text)
    except ValueError:
        return None


def _fetch_html(url: str, timeout: int = 30) -> str:
    response = requests.get(url, headers=REQUEST_HEADERS, timeout=timeout)
    response.raise_for_status()
    response.encoding = response.apparent_encoding or "utf-8"
    logger.info("Fetched %s (%d bytes)", url, len(response.text))
    return response.text


def _fetch_pse_entities(url: str, params: dict[str, object] | None = None, timeout: int = 30) -> list[dict]:
    response = requests.get(url, headers=API_HEADERS, params=params, timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    values = list(payload.get("value", []))
    next_link = payload.get("nextLink")

    while next_link:
        response = requests.get(next_link, headers=API_HEADERS, timeout=timeout)
        response.raise_for_status()
        payload = response.json()
        values.extend(payload.get("value", []))
        next_link = payload.get("nextLink")

    logger.info("Fetched %d API rows from %s", len(values), url)
    return values


def _get_tables_for_date(
    source_url: str,
    session_date: datetime,
    table_cache: dict[str, list[pd.DataFrame]],
) -> tuple[str, list[pd.DataFrame]]:
    dated_url = _date_url(source_url, session_date)
    if dated_url not in table_cache:
        table_cache[dated_url] = _read_tables_from_html(_fetch_html(dated_url))
    return dated_url, table_cache[dated_url]


def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()
    if isinstance(normalized.columns, pd.MultiIndex):
        columns = []
        for col in normalized.columns:
            parts = [
                str(part).strip()
                for part in col
                if str(part).strip() and "Unnamed" not in str(part)
            ]
            columns.append(" | ".join(parts) if parts else str(col[-1]).strip())
        normalized.columns = columns
    else:
        normalized.columns = [str(column).strip() for column in normalized.columns]
    return normalized


def _read_tables_from_html(html: str) -> list[pd.DataFrame]:
    try:
        tables = pd.read_html(StringIO(html), flavor="lxml", decimal=",", thousands=" ")
    except ValueError:
        return []
    return [_flatten_columns(table) for table in tables]


def _resolve_sources(config: dict) -> dict[str, str]:
    scraping_cfg = config.get("scraping", {})
    sources = dict(DEFAULT_SOURCES)

    explicit_sources = scraping_cfg.get("sources", {})
    if isinstance(explicit_sources, dict):
        for key, value in explicit_sources.items():
            if value:
                sources[key] = value

    return sources


def _find_column(columns: Iterable[object], patterns: Iterable[str]) -> str | None:
    normalized = {str(column): _normalize_label(column) for column in columns}
    for pattern in patterns:
        pattern_norm = _normalize_label(pattern)
        for column, column_norm in normalized.items():
            if pattern_norm in column_norm:
                return column
    return None


def _format_query_date(date_value: datetime) -> str:
    return date_value.strftime("%d-%m-%Y")


def _date_url(base_url: str, date_value: datetime) -> str:
    separator = "&" if "?" in base_url else "?"
    return f"{base_url}{separator}dateShow={_format_query_date(date_value)}"


def _candidate_session_dates(fetch_time: datetime, days_back: int = 7) -> list[datetime]:
    base = datetime(fetch_time.year, fetch_time.month, fetch_time.day)
    return [base - timedelta(days=offset) for offset in range(days_back + 1)]


def _pick_numeric_value(row: pd.Series, patterns: list[str]) -> tuple[float | None, str]:
    for pattern in patterns:
        for column in row.index:
            if pattern in _normalize_label(column):
                value = _to_float(row.get(column))
                if value is not None:
                    return value, str(column)
    return None, ""


def _select_row_by_exact_keyword(
    table: pd.DataFrame,
    keyword: str,
    column: str | None = None,
) -> pd.Series | None:
    keyword_norm = _normalize_label(keyword)
    if column and column in table.columns:
        for _, row in table.iterrows():
            if _normalize_label(row.get(column)) == keyword_norm:
                return row
    for _, row in table.iterrows():
        cells = [_normalize_label(value) for value in row.tolist()]
        if any(cell == keyword_norm for cell in cells):
            return row
    return None


def _build_energy_base_record(
    table: pd.DataFrame,
    fetch_time: datetime,
    report_date: datetime,
    source_url: str,
) -> dict[str, object] | None:
    contract_col = _find_column(table.columns, ["kontrakt", "contract", "produkt"])
    if not contract_col:
        return None

    found: dict[int, dict[str, object]] = {}
    for year in (2027, 2028):
        token = f"base_y-{str(year)[-2:]}"
        row = None
        for _, candidate in table.iterrows():
            if token in _normalize_label(candidate.get(contract_col)):
                row = candidate
                break
        if row is None:
            continue

        price, price_column = _pick_numeric_value(
            row,
            ["dkr", "kurs pierwszej transakcji", "kurs min", "kurs maks", "wartość", "wartosc"],
        )
        if price is None:
            continue

        found[year] = {
            "contract": row.get(contract_col),
            "price": price,
            "price_column": price_column,
        }

    if not found:
        return None

    record: dict[str, object] = {
        "Data_Raportu": report_date.strftime("%Y-%m-%d"),
        "Data_Pobrania": fetch_time.strftime("%Y-%m-%d %H:%M:%S"),
        "Zrodlo_URL": source_url,
    }

    for year in (2027, 2028):
        current = found.get(year)
        record[f"Kontrakt_{year}"] = current.get("contract") if current else ""
        record[f"Cena_BASE_{year}_PLN_MWh"] = current.get("price") if current else None
        record[f"Data_Notowania_{year}"] = report_date.strftime("%Y-%m-%d") if current else None
        record[f"Kolumna_Ceny_{year}"] = current.get("price_column") if current else ""
        record[f"Status_{year}"] = "OK" if current else "Brak notowania dla tej sesji"
        record[f"URL_Notowania_{year}"] = source_url if current else ""

    return record


def _build_energy_base_snapshot(
    source_url: str,
    fetch_time: datetime,
    table_cache: dict[str, list[pd.DataFrame]],
    days_back: int = 7,
) -> dict[str, object] | None:
    latest_session_record: dict[str, object] | None = None
    latest_session_url = ""

    for session_date in _candidate_session_dates(fetch_time, days_back=days_back):
        dated_url, tables = _get_tables_for_date(source_url, session_date, table_cache)
        if not tables:
            continue

        record = _build_energy_base_record(
            table=tables[0],
            fetch_time=fetch_time,
            report_date=session_date,
            source_url=dated_url,
        )
        if record:
            latest_session_record = record
            latest_session_url = dated_url
            break

    if latest_session_record is None:
        return None

    snapshot = dict(latest_session_record)
    snapshot["Data_Raportu"] = fetch_time.strftime("%Y-%m-%d")
    snapshot["Data_Pobrania"] = fetch_time.strftime("%Y-%m-%d %H:%M:%S")
    snapshot["Zrodlo_URL"] = source_url

    for year in (2027, 2028):
        has_price = snapshot.get(f"Cena_BASE_{year}_PLN_MWh") is not None
        if has_price:
            snapshot[f"URL_Notowania_{year}"] = latest_session_url
        else:
            snapshot[f"Status_{year}"] = "Brak aktywnego notowania w ostatnich dostepnych sesjach"

    return snapshot


def _build_energy_base_frame(source_url: str, fetch_time: datetime, history_days: int = TGE_HISTORY_DAYS) -> pd.DataFrame:
    records: list[dict[str, object]] = []
    table_cache: dict[str, list[pd.DataFrame]] = {}

    snapshot = _build_energy_base_snapshot(source_url, fetch_time, table_cache)
    if snapshot:
        records.append(snapshot)

    for offset in range(history_days - 1, -1, -1):
        session_date = datetime.combine(fetch_time.date() - timedelta(days=offset), datetime.min.time())
        dated_url, tables = _get_tables_for_date(source_url, session_date, table_cache)
        if not tables:
            continue

        record = _build_energy_base_record(tables[0], fetch_time, session_date, dated_url)
        if record:
            records.append(record)

    if not records:
        raise ValueError("Could not parse BASE history from TGE")

    return pd.DataFrame(records)


def _build_gas_index_record(
    target_table: pd.DataFrame,
    fetch_time: datetime,
    report_date: datetime,
    source_url: str,
    index_keyword: str,
    value_prefix: str,
) -> dict[str, object] | None:
    index_col = _find_column(target_table.columns, ["indeks"])
    row = _select_row_by_exact_keyword(target_table, index_keyword, column=index_col)
    if row is None:
        return None

    price, source_column = _pick_numeric_value(row, ["kurs", "ostatnio", "price"])
    if price is None:
        return None

    change_column = _find_column(target_table.columns, ["zmiana", "change"])
    volume_column = _find_column(target_table.columns, ["wolumen", "volume"])

    record: dict[str, object] = {
        "Data_Raportu": report_date.strftime("%Y-%m-%d"),
        "Data_Pobrania": fetch_time.strftime("%Y-%m-%d %H:%M:%S"),
        "Data_Notowania": report_date.strftime("%Y-%m-%d"),
        "Indeks": index_keyword,
        "Cena_Biezaca_PLN_MWh": price,
        "Zmiana_Proc": _to_float(row.get(change_column)) if change_column else None,
        "Wolumen_MWh": _to_float(row.get(volume_column)) if volume_column else None,
        "Kolumna_Zrodla_Ceny": source_column,
        "Zrodlo_URL": source_url,
    }

    for column in (index_col, change_column, volume_column):
        if column:
            normalized_column = re.sub(r"[^A-Za-z0-9]+", "_", str(column)).strip("_")
            if normalized_column:
                record[f"{value_prefix}_{normalized_column}"] = row.get(column)

    return record


def _build_gas_index_frame(
    source_url: str,
    fetch_time: datetime,
    index_keyword: str,
    value_prefix: str,
    history_days: int = TGE_HISTORY_DAYS,
) -> pd.DataFrame:
    records: list[dict[str, object]] = []
    table_cache: dict[str, list[pd.DataFrame]] = {}

    for offset in range(history_days - 1, -1, -1):
        session_date = datetime.combine(fetch_time.date() - timedelta(days=offset), datetime.min.time())
        dated_url, tables = _get_tables_for_date(source_url, session_date, table_cache)
        if not tables:
            continue

        target_table = None
        for table in tables:
            index_col = _find_column(table.columns, ["indeks"])
            price_col = _find_column(table.columns, ["kurs"])
            if index_col and price_col:
                target_table = table
                break

        if target_table is None:
            continue

        record = _build_gas_index_record(
            target_table=target_table,
            fetch_time=fetch_time,
            report_date=session_date,
            source_url=dated_url,
            index_keyword=index_keyword,
            value_prefix=value_prefix,
        )
        if record:
            records.append(record)

    if not records:
        raise ValueError(f"Could not find a published index row for {index_keyword}")

    return pd.DataFrame(records)


def _build_power_spot_history_frame(source_url: str, fetch_time: datetime, history_days: int = 30) -> pd.DataFrame:
    start_date = (fetch_time.date() - timedelta(days=history_days - 1)).strftime("%Y-%m-%d")
    end_date = fetch_time.date().strftime("%Y-%m-%d")
    params = {
        "$filter": f"business_date ge '{start_date}' and business_date le '{end_date}'",
        "$orderby": "business_date asc,dtime asc",
        "$first": 500,
    }

    rows = _fetch_pse_entities(source_url, params=params)
    if not rows:
        raise ValueError("PSE API returned no spot rows")

    history = pd.DataFrame(rows)
    if history.empty or "csdac_pln" not in history.columns:
        raise ValueError("PSE API payload does not expose csdac_pln")

    history["Cena_Kwadrans_PLN_MWh"] = pd.to_numeric(history["csdac_pln"], errors="coerce")
    history["Data_Dostawy"] = history["business_date"].astype(str)
    history["Dtime"] = pd.to_datetime(history["dtime"], errors="coerce")
    history["Data_Publikacji"] = pd.to_datetime(history["publication_ts"], errors="coerce")
    history = history.dropna(subset=["Cena_Kwadrans_PLN_MWh", "Dtime"]).copy()

    history["Kwadrans_Od"] = history["Dtime"] - pd.Timedelta(minutes=15)
    history["Godzina_Od"] = history["Kwadrans_Od"].dt.floor("h")
    history["Godzina_Do"] = history["Godzina_Od"] + pd.Timedelta(hours=1)

    grouped = (
        history.groupby(["Data_Dostawy", "Godzina_Od", "Godzina_Do"], as_index=False)
        .agg(
            Cena_SPOT_PLN_MWh=("Cena_Kwadrans_PLN_MWh", "mean"),
            Cena_Min_Kwadrans_PLN_MWh=("Cena_Kwadrans_PLN_MWh", "min"),
            Cena_Max_Kwadrans_PLN_MWh=("Cena_Kwadrans_PLN_MWh", "max"),
            Liczba_Kwadransow=("Cena_Kwadrans_PLN_MWh", "count"),
            Data_Publikacji=("Data_Publikacji", "max"),
        )
    )

    grouped["Godzina_Od"] = grouped["Godzina_Od"].dt.strftime("%Y-%m-%d %H:%M:%S")
    grouped["Godzina_Do"] = grouped["Godzina_Do"].dt.strftime("%Y-%m-%d %H:%M:%S")
    grouped["Godzina_Label"] = (
        pd.to_datetime(grouped["Godzina_Od"]).dt.strftime("%H:%M")
        + " - "
        + pd.to_datetime(grouped["Godzina_Do"]).dt.strftime("%H:%M")
    )
    grouped["Data_Publikacji"] = grouped["Data_Publikacji"].dt.strftime("%Y-%m-%d %H:%M:%S")
    grouped["Data_Pobrania"] = fetch_time.strftime("%Y-%m-%d %H:%M:%S")
    grouped["Zrodlo_URL"] = source_url
    grouped["Interwal_Zrodla"] = "PSE CSDAC 15m -> agregacja do 1h"

    return grouped[
        [
            "Data_Dostawy",
            "Godzina_Od",
            "Godzina_Do",
            "Godzina_Label",
            "Cena_SPOT_PLN_MWh",
            "Cena_Min_Kwadrans_PLN_MWh",
            "Cena_Max_Kwadrans_PLN_MWh",
            "Liczba_Kwadransow",
            "Data_Publikacji",
            "Data_Pobrania",
            "Zrodlo_URL",
            "Interwal_Zrodla",
        ]
    ]


def _build_co2_history_frame(source_url: str, fetch_time: datetime) -> pd.DataFrame:
    html = _fetch_html(source_url)
    soup = BeautifulSoup(html, "html.parser")

    selected_rows: list[dict[str, object]] = []
    selected_table = None
    for table in soup.find_all("table"):
        header_text = " ".join(
            _normalize_label(th.get_text(" ", strip=True))
            for th in table.find_all("th")
        )
        if "data" in header_text and ("ostatnio" in header_text or "price" in header_text):
            selected_table = table
            break

    if selected_table is None:
        raise ValueError("CO2 historical table not found in HTML")

    for row in selected_table.find_all("tr"):
        cells = [cell.get_text(" ", strip=True) for cell in row.find_all(["td", "th"])]
        if len(cells) < 2:
            continue
        if _normalize_label(cells[0]) in {"data", "date"}:
            continue
        price = _to_float(cells[1])
        if price is None:
            continue
        parsed_date = pd.to_datetime(cells[0], dayfirst=True, errors="coerce")
        if pd.isna(parsed_date):
            continue
        change_value = _to_float(cells[6]) if len(cells) > 6 else None
        selected_rows.append(
            {
                "Data": parsed_date.strftime("%Y-%m-%d"),
                "Cena_CO2": price,
                "Zmiana_Proc": change_value,
                "Data_Pobrania": fetch_time.strftime("%Y-%m-%d %H:%M:%S"),
                "Zrodlo_URL": source_url,
            }
        )

    if not selected_rows:
        raise ValueError("CO2 history rows were not parsed")

    history = pd.DataFrame(selected_rows)
    history["Data"] = pd.to_datetime(history["Data"])
    min_date = (fetch_time - timedelta(days=40)).date()
    history = history[history["Data"].dt.date >= min_date].copy()
    history = history.sort_values("Data").drop_duplicates(subset=["Data"], keep="last")
    history["Data"] = history["Data"].dt.strftime("%Y-%m-%d")
    return history.reset_index(drop=True)


def _safe_dataset(loader, dataset_name: str) -> pd.DataFrame:
    try:
        return loader()
    except Exception as exc:
        logger.exception("Dataset '%s' failed: %s", dataset_name, exc)
        return pd.DataFrame()


def scrape_all(config: dict) -> dict[str, pd.DataFrame]:
    """
    Fetches all datasets needed by the report.

    Returned keys:
    - co2_history
    - energy_base_history
    - power_spot_history
    - gas_spot_history
    """
    fetch_time = datetime.now()
    sources = _resolve_sources(config)

    results: dict[str, pd.DataFrame] = {}
    results["energy_base_history"] = _safe_dataset(
        lambda: _build_energy_base_frame(sources["energy_base"], fetch_time),
        "energy_base_history",
    )
    results["power_spot_history"] = _safe_dataset(
        lambda: _build_power_spot_history_frame(sources["power_spot"], fetch_time, history_days=30),
        "power_spot_history",
    )
    results["gas_spot_history"] = _safe_dataset(
        lambda: _build_gas_index_frame(
            source_url=sources["gas_spot"],
            fetch_time=fetch_time,
            index_keyword="TGEgasDA",
            value_prefix="Gaz",
        ),
        "gas_spot_history",
    )
    results["co2_history"] = _safe_dataset(
        lambda: _build_co2_history_frame(sources["co2_history"], fetch_time),
        "co2_history",
    )

    logger.info(
        "Scraping completed for %d datasets with %d non-empty results.",
        len(results),
        sum(1 for df in results.values() if df is not None and not df.empty),
    )
    return results
