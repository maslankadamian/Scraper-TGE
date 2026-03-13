"""
Excel workbook builder for the daily market report.

The workbook keeps rolling history and exposes dedicated sheets for:
- current report summary
- CO2 history and 7/30 day views
- yearly BASE energy snapshots
- electricity spot history
- gas history and 7/30 day views
"""
from __future__ import annotations

import logging
from datetime import timedelta
from pathlib import Path

import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

logger = logging.getLogger(__name__)

SHEETS = {
    "report": "Raport_dzienny",
    "co2_history": "CO2_historia",
    "co2_7d": "CO2_7D",
    "co2_30d": "CO2_30D",
    "energy_base": "Energia_BASE_hist",
    "power_spot": "Spot_energia_hist",
    "gas_history": "Gaz_historia",
    "gas_7d": "Gaz_7D",
    "gas_30d": "Gaz_30D",
}


def _apply_header_style(ws) -> None:
    header_fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
    header_font = Font(color="FFFFFF", bold=True)

    for cell in ws[1]:
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

    ws.freeze_panes = "A2"


def _auto_column_width(ws) -> None:
    for col_idx, col in enumerate(ws.columns, start=1):
        max_len = 0
        for cell in col:
            try:
                cell_len = len(str(cell.value)) if cell.value is not None else 0
                max_len = max(max_len, cell_len)
            except Exception:
                pass
        ws.column_dimensions[get_column_letter(col_idx)].width = max(10, min(max_len + 2, 50))


def _resolve_output_path(config: dict) -> Path:
    data_cfg = config.get("data", {})
    filename = data_cfg.get("output_file", "tge_dane_historyczne.xlsx")
    output_dir = data_cfg.get("output_dir", "") or ""
    if output_dir:
        path = Path(output_dir) / filename
        path.parent.mkdir(parents=True, exist_ok=True)
    else:
        path = Path(filename)
    return path


def _read_existing_sheets(path: Path) -> dict[str, pd.DataFrame]:
    if not path.exists():
        return {}

    sheets: dict[str, pd.DataFrame] = {}
    try:
        xf = pd.ExcelFile(path, engine="openpyxl")
        for sheet in xf.sheet_names:
            sheets[sheet] = xf.parse(sheet)
        xf.close()
    except Exception as exc:
        logger.warning("Could not read existing workbook %s: %s", path, exc)
    return sheets


def _to_datetime_series(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series([pd.NaT] * len(df), index=df.index)

    series = pd.to_datetime(df[column], errors="coerce", dayfirst=True)
    if series.isna().all():
        series = pd.to_datetime(df[column], errors="coerce")
    return series


def _numeric_series(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series(dtype="float64")
    return pd.to_numeric(df[column], errors="coerce")


def _sort_if_possible(df: pd.DataFrame, columns: list[str], ascending: bool = False) -> pd.DataFrame:
    existing_columns = [column for column in columns if column in df.columns]
    if not existing_columns:
        return df.copy()
    return df.sort_values(by=existing_columns, ascending=ascending)


def _merge_history(
    existing: pd.DataFrame | None,
    incoming: pd.DataFrame | None,
    key_columns: list[str],
    sort_columns: list[str],
) -> pd.DataFrame:
    frames = []
    if existing is not None and not existing.empty:
        frames.append(existing.copy())
    if incoming is not None and not incoming.empty:
        frames.append(incoming.copy())

    if not frames:
        return pd.DataFrame()

    merged = pd.concat(frames, ignore_index=True, sort=False)

    sort_helpers: list[str] = []
    for column in sort_columns:
        helper = f"__sort_{column}"
        merged[helper] = _to_datetime_series(merged, column)
        sort_helpers.append(helper)

    merged = merged.sort_values(sort_helpers)
    existing_keys = [column for column in key_columns if column in merged.columns]
    if existing_keys:
        merged = merged.drop_duplicates(subset=existing_keys, keep="last")
    merged = merged.drop(columns=sort_helpers)
    merged = merged.reset_index(drop=True)
    return merged


def _last_days(df: pd.DataFrame, date_column: str, days: int) -> pd.DataFrame:
    if df.empty or date_column not in df.columns:
        return df.copy()

    subset = df.copy()
    subset["__date"] = _to_datetime_series(subset, date_column)
    subset = subset.dropna(subset=["__date"])
    if subset.empty:
        return df.iloc[0:0].copy()

    latest_date = subset["__date"].max().normalize()
    cutoff = latest_date - timedelta(days=days - 1)
    subset = subset[subset["__date"] >= cutoff].copy()
    subset = subset.sort_values("__date", ascending=False).drop(columns=["__date"])
    return subset.reset_index(drop=True)


def _latest_row(df: pd.DataFrame, date_column: str) -> pd.Series | None:
    if df.empty or date_column not in df.columns:
        return None

    subset = df.copy()
    subset["__date"] = _to_datetime_series(subset, date_column)
    subset = subset.dropna(subset=["__date"])
    subset = subset.sort_values("__date")
    if subset.empty:
        return None
    return subset.iloc[-1]


def _format_value(value: object, decimals: int = 2) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "brak"
    if isinstance(value, (int, float)):
        return f"{value:.{decimals}f}"
    return str(value)


def _range_sheet_name(section: str, window: int) -> str:
    if section == "CO2":
        return SHEETS["co2_7d"] if window == 7 else SHEETS["co2_30d"]
    return SHEETS["gas_7d"] if window == 7 else SHEETS["gas_30d"]


def _build_range_rows(
    section: str,
    history: pd.DataFrame,
    value_column: str,
    date_column: str,
    unit: str,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    latest = _latest_row(history, date_column)

    if latest is None:
        rows.append(
            {
                "Sekcja": section,
                "Metryka": "Status",
                "Wartosc": "brak danych",
                "Jednostka": unit,
                "Data": "",
                "Uwagi": "Arkusz zostanie uzupelniony po pierwszym poprawnym pobraniu.",
            }
        )
        return rows

    rows.append(
        {
            "Sekcja": section,
            "Metryka": "Cena biezaca",
            "Wartosc": latest.get(value_column),
            "Jednostka": unit,
            "Data": latest.get(date_column, ""),
            "Uwagi": "Najnowsza dostepna wartosc.",
        }
    )

    for window in (7, 30):
        scoped = _last_days(history, date_column, window)
        values = _numeric_series(scoped, value_column).dropna()
        note = f"Szczegoly w arkuszu {_range_sheet_name(section, window)}"

        if values.empty:
            rows.append(
                {
                    "Sekcja": section,
                    "Metryka": f"Zakres {window}D",
                    "Wartosc": "brak danych",
                    "Jednostka": unit,
                    "Data": latest.get(date_column, ""),
                    "Uwagi": "Historia jest jeszcze zbyt krotka.",
                }
            )
            continue

        rows.extend(
            [
                {
                    "Sekcja": section,
                    "Metryka": f"Minimum {window}D",
                    "Wartosc": values.min(),
                    "Jednostka": unit,
                    "Data": latest.get(date_column, ""),
                    "Uwagi": note,
                },
                {
                    "Sekcja": section,
                    "Metryka": f"Maksimum {window}D",
                    "Wartosc": values.max(),
                    "Jednostka": unit,
                    "Data": latest.get(date_column, ""),
                    "Uwagi": note,
                },
                {
                    "Sekcja": section,
                    "Metryka": f"Srednia {window}D",
                    "Wartosc": values.mean(),
                    "Jednostka": unit,
                    "Data": latest.get(date_column, ""),
                    "Uwagi": f"Liczba obserwacji: {len(values)}",
                },
            ]
        )

    return rows


def _build_report_sheet(
    co2_history: pd.DataFrame,
    energy_history: pd.DataFrame,
    power_spot_history: pd.DataFrame,
    gas_history: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    rows.extend(_build_range_rows("CO2", co2_history, "Cena_CO2", "Data", "EUR"))

    latest_energy = _latest_row(energy_history, "Data_Raportu")
    if latest_energy is None:
        rows.append(
            {
                "Sekcja": "Energia BASE",
                "Metryka": "Status",
                "Wartosc": "brak danych",
                "Jednostka": "PLN/MWh",
                "Data": "",
                "Uwagi": "Brak danych o kontraktach BASE.",
            }
        )
    else:
        for year in (2026, 2027, 2028):
            rows.append(
                {
                    "Sekcja": "Energia BASE",
                    "Metryka": str(year),
                    "Wartosc": latest_energy.get(f"Cena_BASE_{year}_PLN_MWh"),
                    "Jednostka": "PLN/MWh",
                    "Data": latest_energy.get(f"Data_Notowania_{year}") or latest_energy.get("Data_Raportu", ""),
                    "Uwagi": latest_energy.get(f"Status_{year}", ""),
                }
            )

    latest_power = _latest_row(power_spot_history, "Data_Raportu")
    if latest_power is None:
        rows.append(
            {
                "Sekcja": "Spot energia",
                "Metryka": "Cena spotowa z dnia",
                "Wartosc": "brak danych",
                "Jednostka": "PLN/MWh",
                "Data": "",
                "Uwagi": "Brak danych z RDN.",
            }
        )
    else:
        rows.append(
            {
                "Sekcja": "Spot energia",
                "Metryka": "Cena spotowa z dnia",
                "Wartosc": latest_power.get("Cena_Biezaca_PLN_MWh"),
                "Jednostka": "PLN/MWh",
                "Data": latest_power.get("Data_Raportu", ""),
                "Uwagi": f"Indeks {latest_power.get('Indeks', 'TGeBASE')}, kolumna: {latest_power.get('Kolumna_Zrodla_Ceny', 'n/d')}",
            }
        )

    rows.extend(_build_range_rows("Gaz", gas_history, "Cena_Biezaca_PLN_MWh", "Data_Raportu", "PLN/MWh"))
    report = pd.DataFrame(rows)
    return report[["Sekcja", "Metryka", "Wartosc", "Jednostka", "Data", "Uwagi"]]


def append_to_excel(scraped: dict[str, pd.DataFrame], config: dict) -> Path:
    output_path = _resolve_output_path(config)
    existing = _read_existing_sheets(output_path)

    co2_history = _merge_history(
        existing.get(SHEETS["co2_history"]),
        scraped.get("co2_history"),
        key_columns=["Data"],
        sort_columns=["Data", "Data_Pobrania"],
    )
    energy_history = _merge_history(
        existing.get(SHEETS["energy_base"]),
        scraped.get("energy_base_history"),
        key_columns=["Data_Raportu"],
        sort_columns=["Data_Raportu", "Data_Pobrania"],
    )
    power_spot_history = _merge_history(
        existing.get(SHEETS["power_spot"]),
        scraped.get("power_spot_history"),
        key_columns=["Data_Raportu", "Indeks"],
        sort_columns=["Data_Raportu", "Data_Pobrania"],
    )
    gas_history = _merge_history(
        existing.get(SHEETS["gas_history"]),
        scraped.get("gas_spot_history"),
        key_columns=["Data_Raportu", "Indeks"],
        sort_columns=["Data_Raportu", "Data_Pobrania"],
    )

    report = _build_report_sheet(co2_history, energy_history, power_spot_history, gas_history)

    sheets = {
        SHEETS["report"]: report,
        SHEETS["co2_history"]: _sort_if_possible(co2_history, ["Data"], ascending=False),
        SHEETS["co2_7d"]: _last_days(co2_history, "Data", 7),
        SHEETS["co2_30d"]: _last_days(co2_history, "Data", 30),
        SHEETS["energy_base"]: _sort_if_possible(energy_history, ["Data_Raportu"], ascending=False),
        SHEETS["power_spot"]: _sort_if_possible(power_spot_history, ["Data_Raportu"], ascending=False),
        SHEETS["gas_history"]: _sort_if_possible(gas_history, ["Data_Raportu"], ascending=False),
        SHEETS["gas_7d"]: _last_days(gas_history, "Data_Raportu", 7),
        SHEETS["gas_30d"]: _last_days(gas_history, "Data_Raportu", 30),
    }

    _write_excel(output_path, sheets)
    logger.info("Saved workbook with %d sheets to %s", len(sheets), output_path)
    return output_path


def _write_excel(path: Path, sheets: dict[str, pd.DataFrame]) -> None:
    with pd.ExcelWriter(str(path), engine="openpyxl") as writer:
        for sheet_name, df in sheets.items():
            frame = df if isinstance(df, pd.DataFrame) else pd.DataFrame()
            frame.to_excel(writer, sheet_name=sheet_name, index=False)

    try:
        wb = load_workbook(str(path))
        for sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            if ws.max_row >= 1:
                _apply_header_style(ws)
                _auto_column_width(ws)
        wb.save(str(path))
    except Exception as exc:
        logger.warning("Excel formatting failed for %s: %s", path, exc)


def get_summary(output_path: Path) -> str:
    if not output_path.exists():
        return "Brak pliku z danymi."

    try:
        sheets = _read_existing_sheets(output_path)
        report = sheets.get(SHEETS["report"], pd.DataFrame())
        co2_7d = sheets.get(SHEETS["co2_7d"], pd.DataFrame())
        co2_30d = sheets.get(SHEETS["co2_30d"], pd.DataFrame())
        gas_7d = sheets.get(SHEETS["gas_7d"], pd.DataFrame())
        gas_30d = sheets.get(SHEETS["gas_30d"], pd.DataFrame())

        lines = [f"Plik: {output_path.name}"]
        lines.append("Arkusze: " + ", ".join(SHEETS.values()))

        if not report.empty:
            energy_rows = report[report["Sekcja"] == "Energia BASE"]
            co2_current = report[(report["Sekcja"] == "CO2") & (report["Metryka"] == "Cena biezaca")]
            spot_current = report[(report["Sekcja"] == "Spot energia") & (report["Metryka"] == "Cena spotowa z dnia")]
            gas_current = report[(report["Sekcja"] == "Gaz") & (report["Metryka"] == "Cena biezaca")]

            if not co2_current.empty:
                row = co2_current.iloc[0]
                lines.append(f"CO2: {_format_value(row['Wartosc'])} {row['Jednostka']} ({row['Data']})")

            if not energy_rows.empty:
                energy_parts = []
                for _, row in energy_rows.iterrows():
                    energy_parts.append(f"{row['Metryka']}: {_format_value(row['Wartosc'])}")
                lines.append("Energia BASE: " + ", ".join(energy_parts))

            if not spot_current.empty:
                row = spot_current.iloc[0]
                lines.append(f"Spot energia: {_format_value(row['Wartosc'])} {row['Jednostka']} ({row['Data']})")

            if not gas_current.empty:
                row = gas_current.iloc[0]
                lines.append(f"Gaz: {_format_value(row['Wartosc'])} {row['Jednostka']} ({row['Data']})")

        lines.append(f"CO2 7D: {len(co2_7d)} rekordow | CO2 30D: {len(co2_30d)} rekordow")
        lines.append(f"Gaz 7D: {len(gas_7d)} rekordow | Gaz 30D: {len(gas_30d)} rekordow")
        return "\n".join(lines)
    except Exception as exc:
        return f"Blad odczytu raportu: {exc}"
