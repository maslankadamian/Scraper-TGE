"""
Moduł scrapingu danych z TGE (Towarowa Giełda Energii).

Strategia (w kolejności):
  1. Bezpośrednie zapytanie HTTP (requests) – działa tak samo jak Power Query.
  2. Selenium – fallback gdy strona wymaga JS lub blokuje requests.
     Obsługuje okna cookie consent.
"""
import logging
import time
from datetime import datetime
from io import StringIO
from typing import Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

logger = logging.getLogger(__name__)

# Nagłówki imitujące normalną przeglądarkę (jak Power Query)
_REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}


# ── Metoda 1: requests ────────────────────────────────────────────────────────

def _fetch_with_requests(url: str, timeout: int = 30) -> Optional[str]:
    """Pobiera HTML przez zwykłe zapytanie HTTP (bez przeglądarki)."""
    try:
        session = requests.Session()
        resp = session.get(url, headers=_REQUEST_HEADERS, timeout=timeout, verify=True)
        resp.raise_for_status()
        resp.encoding = resp.apparent_encoding or "utf-8"
        logger.info("requests OK (%d bytes) dla: %s", len(resp.text), url)
        return resp.text
    except requests.exceptions.SSLError:
        # Spróbuj bez weryfikacji SSL (niektóre serwery mają błędne certyfikaty)
        try:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            session = requests.Session()
            resp = session.get(url, headers=_REQUEST_HEADERS, timeout=timeout, verify=False)
            resp.raise_for_status()
            resp.encoding = resp.apparent_encoding or "utf-8"
            logger.warning("requests OK (bez SSL) dla: %s", url)
            return resp.text
        except Exception as exc2:
            logger.warning("requests (bez SSL) nieudane dla %s: %s", url, exc2)
            return None
    except Exception as exc:
        logger.warning("requests nieudane dla %s: %s", url, exc)
        return None


# ── Metoda 2: Selenium ────────────────────────────────────────────────────────

def _build_driver(config: dict) -> webdriver.Chrome:
    """Tworzy i konfiguruje instancję ChromeDriver."""
    scraping_cfg = config.get("scraping", {})
    options = Options()

    if scraping_cfg.get("headless", True):
        options.add_argument("--headless=new")

    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    user_agent = scraping_cfg.get(
        "user_agent",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    )
    options.add_argument(f"--user-agent={user_agent}")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--lang=pl-PL")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    driver.execute_cdp_cmd(
        "Page.addScriptToEvaluateOnNewDocument",
        {
            "source": (
                "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
            )
        },
    )
    return driver


def _accept_cookies(driver: webdriver.Chrome) -> None:
    """Próbuje zaakceptować okno cookie consent (GDPR)."""
    selectors = [
        # CookieBot (popularny w Polsce)
        (By.ID, "CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll"),
        # Inne częste wzorce – szukaj po tekście przycisku
        (By.XPATH, "//button[contains(translate(., 'AKCEPTUJZGADZMSIĘ', 'akceptujzgadzmsię'), 'akceptuj')]"),
        (By.XPATH, "//button[contains(translate(., 'AKCEPTUJZGADZMSIĘ', 'akceptujzgadzmsię'), 'zgadzam')]"),
        (By.XPATH, "//button[contains(translate(., 'AKCEPTUJZGADZMSIĘ', 'akceptujzgadzmsię'), 'accept')]"),
        (By.XPATH, "//button[contains(translate(., 'AKCEPTUJZGADZMSIĘ', 'akceptujzgadzmsię'), 'zezwól')]"),
        (By.XPATH, "//button[contains(@class, 'cookie') and contains(@class, 'accept')]"),
        (By.CSS_SELECTOR, "[id*='cookie'] button, [class*='cookie-accept']"),
    ]

    for by, selector in selectors:
        try:
            btn = WebDriverWait(driver, 3).until(
                EC.element_to_be_clickable((by, selector))
            )
            btn.click()
            logger.info("Zaakceptowano cookies.")
            time.sleep(1)
            return
        except Exception:
            pass


# ── Parsowanie tabel ──────────────────────────────────────────────────────────

def _extract_tables_from_html(
    html: str,
    url: str,
    fetch_time: datetime,
    date_column: str,
    min_rows: int = 1,
    min_cols: int = 2,
) -> list[pd.DataFrame]:
    """
    Parsuje HTML i zwraca listę DataFrame-ów z tabelami zawierającymi dane.
    Tabele layoutowe (za mało kolumn/wierszy) są pomijane.
    """
    soup = BeautifulSoup(html, "lxml")
    raw_tables = soup.find_all("table")
    logger.info("Znaleziono %d elementów <table> na: %s", len(raw_tables), url)

    result = []
    for idx, table in enumerate(raw_tables):
        try:
            dfs = pd.read_html(StringIO(str(table)), flavor="lxml", thousands="\xa0")
            if not dfs:
                continue
            df = dfs[0]

            # Spłaszcz wielopoziomowe nagłówki (np. tabele z grupowaniem kolumn)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [
                    " | ".join(str(c) for c in col if str(c) not in ("nan", ""))
                    .strip()
                    for col in df.columns
                ]

            df.columns = [str(c).strip() for c in df.columns]

            # Usuń całkowicie puste wiersze i kolumny
            df.dropna(how="all", inplace=True)
            df.dropna(axis=1, how="all", inplace=True)

            rows, cols = df.shape
            if cols < min_cols or rows < min_rows:
                logger.debug(
                    "Pomijam tabelę %d (%d w × %d k) – wygląda na layoutową",
                    idx + 1, rows, cols,
                )
                continue

            # Metadane
            df[date_column] = fetch_time.strftime("%Y-%m-%d %H:%M:%S")
            df["Zrodlo_URL"] = url
            df["Numer_Tabeli"] = idx + 1

            result.append(df)
            logger.info(
                "Tabela %d: %d wierszy × %d kolumn | Kolumny: %s",
                idx + 1, rows, cols, list(df.columns[:8]),
            )

        except Exception as exc:
            logger.warning("Nie można sparsować tabeli %d z %s: %s", idx + 1, url, exc)

    return result


# ── Scraping jednego URL ──────────────────────────────────────────────────────

def scrape_url(
    driver: webdriver.Chrome,
    url: str,
    config: dict,
    fetch_time: datetime,
) -> list[pd.DataFrame]:
    """
    Pobiera tabele z podanego URL.

    Krok 1: requests (szybko, bez przeglądarki – tak działa Power Query).
    Krok 2: Selenium (wolniej, obsługuje JS i cookie consent).
    """
    scraping_cfg = config.get("scraping", {})
    page_load_timeout = scraping_cfg.get("page_load_timeout", 30)
    wait_for_js = scraping_cfg.get("wait_for_js", 5)
    retry_count = scraping_cfg.get("retry_count", 3)
    date_column = config.get("data", {}).get("date_column", "Data_Pobrania")

    # ── Krok 1: requests ──────────────────────────────────────────────────────
    logger.info("Próba 1/2 (requests): %s", url)
    html = _fetch_with_requests(url, timeout=page_load_timeout)
    if html:
        tables = _extract_tables_from_html(html, url, fetch_time, date_column)
        if tables:
            logger.info("requests: znaleziono %d tabel z %s", len(tables), url)
            return tables
        logger.info(
            "requests: HTML pobrano, ale brak tabel z danymi. "
            "Strona może wymagać JS – przełączam na Selenium."
        )
    else:
        logger.info("requests: nie udało się pobrać HTML. Przełączam na Selenium.")

    # ── Krok 2: Selenium ──────────────────────────────────────────────────────
    for attempt in range(1, retry_count + 1):
        try:
            logger.info("Próba 2/2 (Selenium %d/%d): %s", attempt, retry_count, url)
            driver.set_page_load_timeout(page_load_timeout)
            driver.get(url)

            # Obsługa okna cookies
            _accept_cookies(driver)

            # Czekaj na załadowanie tabel
            try:
                WebDriverWait(driver, wait_for_js + 5).until(
                    EC.presence_of_element_located((By.TAG_NAME, "table"))
                )
                logger.debug("Tabela znaleziona w DOM.")
            except Exception:
                logger.debug("Nie wykryto tabeli w %ds – próbuję parsować.", wait_for_js + 5)

            # Czas na wykonanie AJAX / lazy-load
            time.sleep(3)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)

            html = driver.page_source
            tables = _extract_tables_from_html(html, url, fetch_time, date_column)

            if tables:
                logger.info("Selenium: znaleziono %d tabel z %s", len(tables), url)
                return tables

            logger.warning("Selenium: brak tabel z danymi na %s", url)
            return []

        except Exception as exc:
            logger.error("Błąd Selenium %s (próba %d/%d): %s", url, attempt, retry_count, exc)
            if attempt < retry_count:
                wait = 2 ** attempt
                logger.info("Czekam %ds przed kolejną próbą...", wait)
                time.sleep(wait)

    logger.error("Wszystkie metody pobrania danych z %s nieudane.", url)
    return []


# ── Scraping wszystkich URL ───────────────────────────────────────────────────

def scrape_all(config: dict) -> dict[str, list[pd.DataFrame]]:
    """
    Główna funkcja scrapingu.
    Pobiera dane ze wszystkich URL-i z konfiguracji.
    Zwraca słownik {url: [DataFrame, ...]}
    """
    urls = config.get("scraping", {}).get("urls", [])
    fetch_time = datetime.now()
    results: dict[str, list[pd.DataFrame]] = {}

    if not urls:
        logger.error("Brak URL-i w konfiguracji (scraping.urls).")
        return results

    driver = None
    try:
        driver = _build_driver(config)

        for url in urls:
            tables = scrape_url(driver, url, config, fetch_time)
            results[url] = tables

    finally:
        if driver:
            driver.quit()
            logger.debug("ChromeDriver zamknięty.")

    total_tables = sum(len(v) for v in results.values())
    logger.info(
        "Scraping zakończony. Pobrano łącznie %d tabeli(e) z %d stron.",
        total_tables,
        len(urls),
    )
    return results
