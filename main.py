"""
TGE Data Scraper – logika scrapingu i harmonogram.

Uruchomienie przez web UI:
  python app.py

Uruchomienie CLI (bez web UI):
  python main.py --run-once       # Tylko jedno pobranie
  python main.py --config inna.yaml
"""
import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

import schedule
import time
import yaml

from data_manager import append_to_excel, get_summary
from scraper import scrape_all

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("tge_scraper.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


# ── Config ───────────────────────────────────────────────────────────────────

def load_config(config_path: str = "config.yaml") -> dict:
    path = Path(config_path)
    if not path.exists():
        logger.error("Plik konfiguracyjny nie istnieje: %s", config_path)
        sys.exit(1)
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    logger.info("Załadowano konfigurację z: %s", config_path)
    return cfg


# ── Główna logika jednego cyklu ───────────────────────────────────────────────

def run_cycle(config: dict) -> dict:
    """
    Jeden pełny cykl:
    1. Scraping tabel z TGE
    2. Zapis/dopisanie do Excel

    Zwraca słownik z wynikiem (używany przez web UI i CLI).
    E-mail NIE jest wysyłany automatycznie – wywoływany ręcznie z app.py.
    """
    start = datetime.now()
    logger.info("=" * 60)
    logger.info("Start cyklu: %s", start.strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("=" * 60)

    # 1. Scraping
    scraped = scrape_all(config)
    if not any(scraped.values()):
        logger.error("Nie pobrano żadnych danych. Cykl przerwany.")
        return {"ok": False, "error": "Nie pobrano żadnych danych.", "start": start}

    # 2. Zapis do Excel
    excel_path = append_to_excel(scraped, config)
    summary = get_summary(excel_path)
    logger.info("Podsumowanie pliku:\n%s", summary)

    elapsed = (datetime.now() - start).total_seconds()
    logger.info("Cykl zakończony w %.1f s.", elapsed)

    return {
        "ok": True,
        "excel_path": excel_path,
        "summary": summary,
        "start": start,
        "elapsed": elapsed,
    }


# ── Harmonogram ───────────────────────────────────────────────────────────────

def setup_schedule(config: dict) -> None:
    """Konfiguruje harmonogram na podstawie config.yaml."""
    sched_cfg = config.get("schedule", {})
    frequency = sched_cfg.get("frequency", "daily").lower()
    run_time = sched_cfg.get("time", "08:00")
    day = sched_cfg.get("day", "monday").lower()

    if frequency == "daily":
        schedule.every().day.at(run_time).do(run_cycle, config)
        logger.info("Harmonogram: codziennie o %s", run_time)

    elif frequency == "weekly":
        days_map = {
            "monday": schedule.every().monday,
            "tuesday": schedule.every().tuesday,
            "wednesday": schedule.every().wednesday,
            "thursday": schedule.every().thursday,
            "friday": schedule.every().friday,
            "saturday": schedule.every().saturday,
            "sunday": schedule.every().sunday,
        }
        scheduler = days_map.get(day, schedule.every().monday)
        scheduler.at(run_time).do(run_cycle, config)
        logger.info("Harmonogram: co tydzień w %s o %s", day, run_time)

    elif frequency == "hourly":
        schedule.every().hour.do(run_cycle, config)
        logger.info("Harmonogram: co godzinę")

    elif frequency == "manual":
        logger.info("Tryb ręczny – harmonogram wyłączony.")

    else:
        logger.warning(
            "Nieznana częstotliwość '%s'. Używam 'daily' o 08:00.", frequency
        )
        schedule.every().day.at("08:00").do(run_cycle, config)


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="TGE Data Scraper CLI – jednorazowe pobranie danych."
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Ścieżka do pliku konfiguracyjnego (domyślnie: config.yaml)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)

    logger.info("Tryb CLI (--run-once). Uruchamiam jednorazowe pobranie.")
    result = run_cycle(config)
    if result["ok"]:
        logger.info("Gotowe. Plik: %s", result["excel_path"])
    else:
        logger.error("Błąd: %s", result.get("error"))
        sys.exit(1)


if __name__ == "__main__":
    main()
