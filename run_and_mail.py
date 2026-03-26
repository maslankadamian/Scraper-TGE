"""
TGE Data Scraper - jednorazowy wyzwalacz scrapowania i wysylki maila.

Alternatywa dla app.py (Flask) - nie wymaga uruchamiania serwera HTTP.

Uzycie:
  python run_and_mail.py                    # scraping + mail
  python run_and_mail.py --no-mail          # tylko scraping, bez maila
  python run_and_mail.py --mail-only        # tylko mail (wymaga istniejacego pliku Excel)
  python run_and_mail.py --config other.yaml
"""
import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

from main import load_config, run_cycle
from email_sender import send_report

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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="TGE Scraper - jednorazowe scrapowanie + wysylka maila (bez serwera HTTP)."
    )
    parser.add_argument(
        "--config", default="config.yaml",
        help="Sciezka do pliku konfiguracyjnego (domyslnie: config.yaml)",
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--no-mail", action="store_true",
        help="Tylko scrapowanie, bez wysylki maila.",
    )
    group.add_argument(
        "--mail-only", action="store_true",
        help="Tylko wysylka maila (plik Excel musi juz istniec).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    config = load_config(args.config)

    excel_path: Path | None = None
    summary = ""
    fetch_time = datetime.now()

    # --- Scraping ---
    if not args.mail_only:
        logger.info("Rozpoczynam scrapowanie danych...")
        result = run_cycle(config)
        if not result["ok"]:
            logger.error("Scrapowanie nie powiodlo sie: %s", result.get("error"))
            sys.exit(1)
        excel_path = result["excel_path"]
        summary = result.get("summary", "")
        fetch_time = result.get("start", fetch_time)
        logger.info("Scrapowanie zakonczone. Plik: %s", excel_path)
    else:
        # Mail-only: find Excel path from config
        output_cfg = config.get("output", {})
        excel_path = Path(output_cfg.get("excel_file", "TGE_dane.xlsx"))
        if not excel_path.exists():
            logger.error("Plik Excel nie istnieje: %s. Uruchom najpierw scrapowanie.", excel_path)
            sys.exit(1)
        logger.info("Tryb --mail-only. Uzywam istniejacego pliku: %s", excel_path)

    # --- Email ---
    if not args.no_mail:
        logger.info("Wysylam raport e-mailem...")
        ok, error = send_report(excel_path, summary, config, fetch_time)
        if ok:
            logger.info("Mail wyslany pomyslnie.")
        else:
            logger.error("Blad wysylki maila: %s", error)
            sys.exit(1)

    logger.info("Gotowe.")


if __name__ == "__main__":
    main()
