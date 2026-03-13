"""
TGE Data Scraper - scraping orchestration and schedule handling.

Run with web UI:
  python app.py

Run as CLI:
  python main.py
  python main.py --config other.yaml
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


def load_config(config_path: str = "config.yaml") -> dict:
    path = Path(config_path)
    if not path.exists():
        logger.error("Plik konfiguracyjny nie istnieje: %s", config_path)
        sys.exit(1)
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    logger.info("Zaladowano konfiguracje z: %s", config_path)
    return cfg


def run_cycle(config: dict) -> dict:
    """
    One end-to-end refresh cycle:
    1. Download source datasets
    2. Update workbook history and report tabs
    """
    start = datetime.now()
    logger.info("=" * 60)
    logger.info("Start cyklu: %s", start.strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("=" * 60)

    scraped = scrape_all(config)
    if not any(dataset is not None and not getattr(dataset, "empty", True) for dataset in scraped.values()):
        logger.error("Nie pobrano zadnych danych. Cykl przerwany.")
        return {"ok": False, "error": "Nie pobrano zadnych danych.", "start": start}

    excel_path = append_to_excel(scraped, config)
    summary = get_summary(excel_path)
    logger.info("Podsumowanie pliku:\n%s", summary)

    elapsed = (datetime.now() - start).total_seconds()
    logger.info("Cykl zakonczony w %.1f s.", elapsed)

    return {
        "ok": True,
        "excel_path": excel_path,
        "summary": summary,
        "start": start,
        "elapsed": elapsed,
    }


def setup_schedule(config: dict) -> None:
    """Configure the scheduler from config.yaml."""
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
        logger.info("Harmonogram: co tydzien w %s o %s", day, run_time)

    elif frequency == "hourly":
        schedule.every().hour.do(run_cycle, config)
        logger.info("Harmonogram: co godzine")

    elif frequency == "manual":
        logger.info("Tryb reczny - harmonogram wylaczony.")

    else:
        logger.warning(
            "Nieznana czestotliwosc '%s'. Uzywam 'daily' o 08:00.", frequency
        )
        schedule.every().day.at("08:00").do(run_cycle, config)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="TGE Data Scraper CLI - jednorazowe pobranie danych."
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Sciezka do pliku konfiguracyjnego (domyslnie: config.yaml)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)

    logger.info("Tryb CLI. Uruchamiam jednorazowe pobranie.")
    result = run_cycle(config)
    if result["ok"]:
        logger.info("Gotowe. Plik: %s", result["excel_path"])
    else:
        logger.error("Blad: %s", result.get("error"))
        sys.exit(1)


if __name__ == "__main__":
    main()
