"""
Moduł zarządzania danymi historycznymi w pliku Excel.
Nowe dane są dopisywane do istniejącego pliku (historia jest zachowana).
Każda strona/tabela trafia na osobny arkusz.
"""
import logging
import os
import re
from pathlib import Path

import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

logger = logging.getLogger(__name__)


def _sheet_name_from_url(url: str, table_idx: int) -> str:
    """Generuje bezpieczną nazwę arkusza (max 31 znaków) z URL i numeru tabeli."""
    # Wyciągnij ostatni segment ścieżki URL
    path = url.rstrip("/").split("/")[-1] or "strona_glowna"
    path = re.sub(r"[^\w\-]", "_", path)
    name = f"{path}_T{table_idx}"
    # Excel ogranicza nazwy arkuszy do 31 znaków
    return name[:31]


def _apply_header_style(ws) -> None:
    """Formatuje nagłówki tabeli w arkuszu."""
    header_fill = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
    header_font = Font(color="FFFFFF", bold=True)

    for cell in ws[1]:
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

    # Zamroź pierwszy wiersz (nagłówki)
    ws.freeze_panes = "A2"


def _auto_column_width(ws) -> None:
    """Automatycznie dopasowuje szerokość kolumn."""
    for col_idx, col in enumerate(ws.columns, start=1):
        max_len = 0
        for cell in col:
            try:
                cell_len = len(str(cell.value)) if cell.value is not None else 0
                max_len = max(max_len, cell_len)
            except Exception:
                pass
        # Min 10, max 50 znaków
        adjusted = max(10, min(max_len + 2, 50))
        ws.column_dimensions[get_column_letter(col_idx)].width = adjusted


def _resolve_output_path(config: dict) -> Path:
    """Zwraca pełną ścieżkę do pliku Excel."""
    data_cfg = config.get("data", {})
    filename = data_cfg.get("output_file", "tge_dane_historyczne.xlsx")
    output_dir = data_cfg.get("output_dir", "") or ""
    if output_dir:
        path = Path(output_dir) / filename
        path.parent.mkdir(parents=True, exist_ok=True)
    else:
        path = Path(filename)
    return path


def append_to_excel(
    scraped: dict[str, list[pd.DataFrame]],
    config: dict,
) -> Path:
    """
    Dopisuje nowo pobrane dane do pliku Excel z historią.

    Każda (URL, numer_tabeli) trafia na osobny arkusz.
    Jeśli arkusz już istnieje, nowe wiersze są dopisywane na dole.
    Jeśli plik nie istnieje, jest tworzony od zera.

    Zwraca ścieżkę do pliku Excel.
    """
    output_path = _resolve_output_path(config)
    date_col = config.get("data", {}).get("date_column", "Data_Pobrania")

    # Zbierz wszystkie (sheet_name -> DataFrame) do zapisania
    new_data: dict[str, pd.DataFrame] = {}
    for url, tables in scraped.items():
        for df in tables:
            table_idx = df["Numer_Tabeli"].iloc[0] if "Numer_Tabeli" in df.columns else 0
            sheet_name = _sheet_name_from_url(url, table_idx)
            new_data[sheet_name] = df

    if not new_data:
        logger.warning("Brak danych do zapisania.")
        return output_path

    if output_path.exists():
        logger.info("Dopisuję do istniejącego pliku: %s", output_path)
        _append_to_existing(output_path, new_data, date_col)
    else:
        logger.info("Tworzę nowy plik: %s", output_path)
        _create_new_file(output_path, new_data)

    logger.info(
        "Zapisano %d arkuszy do %s",
        len(new_data), output_path
    )
    return output_path


def _append_to_existing(
    path: Path,
    new_data: dict[str, pd.DataFrame],
    date_col: str,
) -> None:
    """Dopisuje nowe wiersze do istniejącego pliku Excel."""
    # Wczytaj istniejące arkusze
    existing_sheets: dict[str, pd.DataFrame] = {}
    try:
        xf = pd.ExcelFile(path, engine="openpyxl")
        for sheet in xf.sheet_names:
            existing_sheets[sheet] = xf.parse(sheet)
        xf.close()
    except Exception as exc:
        logger.error("Błąd odczytu pliku Excel: %s. Tworzę nowy.", exc)
        _create_new_file(path, new_data)
        return

    # Scal istniejące z nowymi
    merged: dict[str, pd.DataFrame] = dict(existing_sheets)
    for sheet_name, df_new in new_data.items():
        if sheet_name in merged:
            df_old = merged[sheet_name]
            # Wyrównaj kolumny – dodaj brakujące w starym DataFrame
            all_cols = list(dict.fromkeys(list(df_old.columns) + list(df_new.columns)))
            df_old = df_old.reindex(columns=all_cols)
            df_new = df_new.reindex(columns=all_cols)
            merged[sheet_name] = pd.concat([df_old, df_new], ignore_index=True)
            logger.debug(
                "Arkusz '%s': +%d wierszy (łącznie %d)",
                sheet_name, len(df_new), len(merged[sheet_name]),
            )
        else:
            merged[sheet_name] = df_new
            logger.debug("Nowy arkusz '%s': %d wierszy", sheet_name, len(df_new))

    _write_excel(path, merged)


def _create_new_file(path: Path, data: dict[str, pd.DataFrame]) -> None:
    """Tworzy nowy plik Excel z przekazanymi danymi."""
    _write_excel(path, data)


def _write_excel(path: Path, sheets: dict[str, pd.DataFrame]) -> None:
    """Zapisuje słownik arkuszy do pliku Excel i aplikuje formatowanie."""
    with pd.ExcelWriter(str(path), engine="openpyxl") as writer:
        for sheet_name, df in sheets.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    # Formatowanie po zapisie
    try:
        wb = load_workbook(str(path))
        for sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            _apply_header_style(ws)
            _auto_column_width(ws)
        wb.save(str(path))
    except Exception as exc:
        logger.warning("Formatowanie Excela nieudane (dane są poprawne): %s", exc)


def get_summary(output_path: Path) -> str:
    """Zwraca krótkie podsumowanie zawartości pliku Excel (do e-maila)."""
    if not output_path.exists():
        return "Brak pliku z danymi."

    lines = [f"Plik: {output_path.name}"]
    try:
        xf = pd.ExcelFile(output_path, engine="openpyxl")
        lines.append(f"Liczba arkuszy: {len(xf.sheet_names)}")
        for sheet in xf.sheet_names:
            df = xf.parse(sheet)
            lines.append(f"  - {sheet}: {len(df)} rekordów")
        xf.close()
    except Exception as exc:
        lines.append(f"Błąd odczytu: {exc}")

    return "\n".join(lines)
