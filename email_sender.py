"""
Email delivery for the Excel market report.
Supports SMTP with STARTTLS for Gmail, Outlook, and similar providers.
"""
from __future__ import annotations

import logging
import smtplib
import ssl
from datetime import datetime, timedelta
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

PLACEHOLDER_RECIPIENT_DOMAINS = {"example.com", "example.org", "example.net", "localhost"}

_SECTION_COLORS = {
    "CO2": "#2E7D32",
    "Energia BASE": "#1565C0",
    "Spot energia": "#E65100",
    "Gaz": "#6A1B9A",
}
_SECTION_ICONS = {
    "CO2": "&#127807;",
    "Energia BASE": "&#9889;",
    "Spot energia": "&#128200;",
    "Gaz": "&#128293;",
}
_SECTION_ORDER = ["CO2", "Energia BASE", "Spot energia", "Gaz"]


def _fmt(value: object) -> str:
    if value is None:
        return "brak"
    if isinstance(value, float):
        if pd.isna(value):
            return "brak"
        return f"{value:.2f}"
    if isinstance(value, int):
        return str(value)
    return str(value)


def _build_svg_chart(dates: list[str], values: list[float], color: str, unit: str) -> str:
    """Build an inline SVG line chart for the last 7 days."""
    if len(values) < 2:
        return ""

    width, height = 560, 140
    pad_left, pad_right, pad_top, pad_bottom = 60, 20, 20, 35

    chart_w = width - pad_left - pad_right
    chart_h = height - pad_top - pad_bottom

    v_min = min(values)
    v_max = max(values)
    v_range = v_max - v_min if v_max != v_min else 1.0

    n = len(values)
    points = []
    for i, v in enumerate(values):
        x = pad_left + (i / (n - 1)) * chart_w
        y = pad_top + chart_h - ((v - v_min) / v_range) * chart_h
        points.append((x, y))

    polyline = " ".join(f"{x:.1f},{y:.1f}" for x, y in points)

    # Y-axis labels (3 ticks)
    y_labels = ""
    for frac in (0.0, 0.5, 1.0):
        val = v_min + frac * v_range
        y_pos = pad_top + chart_h - frac * chart_h
        y_labels += (
            f"<text x='{pad_left - 6}' y='{y_pos + 4}' text-anchor='end' "
            f"font-size='10' fill='#999'>{val:.2f}</text>"
            f"<line x1='{pad_left}' y1='{y_pos}' x2='{width - pad_right}' y2='{y_pos}' "
            f"stroke='#eee' stroke-width='1'/>"
        )

    # X-axis date labels
    x_labels = ""
    step = max(1, (n - 1) // min(6, n - 1))
    for i in range(0, n, step):
        x_pos = pad_left + (i / (n - 1)) * chart_w
        label = dates[i][-5:] if len(dates[i]) > 5 else dates[i]
        x_labels += (
            f"<text x='{x_pos}' y='{height - 5}' text-anchor='middle' "
            f"font-size='10' fill='#999'>{label}</text>"
        )

    # Dots on data points
    dots = ""
    for x, y in points:
        dots += f"<circle cx='{x:.1f}' cy='{y:.1f}' r='3' fill='{color}'/>"

    return (
        f"<div style='margin:8px 16px 14px 16px;'>"
        f"<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 {width} {height}' "
        f"width='{width}' height='{height}' style='max-width:100%;background:#fafafa;"
        f"border-radius:6px;'>"
        f"{y_labels}{x_labels}"
        f"<polyline points='{polyline}' fill='none' stroke='{color}' stroke-width='2.5' "
        f"stroke-linejoin='round' stroke-linecap='round'/>"
        f"{dots}"
        f"<text x='{width - pad_right}' y='{pad_top - 4}' text-anchor='end' "
        f"font-size='10' fill='#bbb'>{unit}, ostatnie 7 dni</text>"
        f"</svg></div>"
    )


def _read_history_sheets(excel_path: Path) -> dict[str, pd.DataFrame]:
    """Read historical data sheets for chart generation."""
    sheets: dict[str, pd.DataFrame] = {}
    try:
        xf = pd.ExcelFile(str(excel_path), engine="openpyxl")
        for name in ("CO2", "Spot_energia_srednia", "Gaz", "Energia_BASE"):
            if name in xf.sheet_names:
                sheets[name] = xf.parse(name)
        xf.close()
    except Exception as exc:
        logger.warning("Could not read history sheets: %s", exc)
    return sheets


def _last_7_days(df: pd.DataFrame, date_col: str, value_col: str) -> tuple[list[str], list[float]]:
    """Extract last 7 days of (date, value) pairs sorted ascending."""
    if df.empty or date_col not in df.columns or value_col not in df.columns:
        return [], []

    subset = df.copy()
    subset["__date"] = pd.to_datetime(subset[date_col], errors="coerce")
    subset["__val"] = pd.to_numeric(subset[value_col], errors="coerce")
    subset = subset.dropna(subset=["__date", "__val"])
    if subset.empty:
        return [], []

    latest = subset["__date"].max().normalize()
    cutoff = latest - timedelta(days=6)
    subset = subset[subset["__date"] >= cutoff].sort_values("__date")
    subset = subset.drop_duplicates(subset=["__date"], keep="last")

    dates = [d.strftime("%m-%d") for d in subset["__date"]]
    values = subset["__val"].tolist()
    return dates, values


def _build_section_charts(section: str, history_sheets: dict[str, pd.DataFrame]) -> str:
    """Build SVG chart HTML for a given section using historical data."""
    color = _SECTION_COLORS.get(section, "#333")

    if section == "CO2":
        df = history_sheets.get("CO2", pd.DataFrame())
        dates, values = _last_7_days(df, "Data", "Cena_CO2")
        return _build_svg_chart(dates, values, color, "EUR")

    if section == "Energia BASE":
        df = history_sheets.get("Energia_BASE", pd.DataFrame())
        charts = ""
        for year in (2027, 2028):
            col = f"Cena_BASE_{year}_PLN_MWh"
            dates, values = _last_7_days(df, "Data_Raportu", col)
            if values:
                charts += (
                    f"<div style='margin:0 16px;font-size:11px;color:#888;'>"
                    f"BASE {year}</div>"
                )
                charts += _build_svg_chart(dates, values, color, "PLN/MWh")
        return charts

    if section == "Spot energia":
        df = history_sheets.get("Spot_energia_srednia", pd.DataFrame())
        dates, values = _last_7_days(df, "Data_Dostawy", "Cena_Srednia_Dzien_PLN_MWh")
        return _build_svg_chart(dates, values, color, "PLN/MWh")

    if section == "Gaz":
        df = history_sheets.get("Gaz", pd.DataFrame())
        dates, values = _last_7_days(df, "Data_Raportu", "Cena_Biezaca_PLN_MWh")
        return _build_svg_chart(dates, values, color, "PLN/MWh")

    return ""


def _build_section_card(section: str, rows: pd.DataFrame) -> str:
    color = _SECTION_COLORS.get(section, "#333")
    icon = _SECTION_ICONS.get(section, "")
    row_html = ""
    for _, r in rows.iterrows():
        metryka = r.get("Metryka", "")
        wartosc = _fmt(r.get("Wartosc"))
        jednostka = r.get("Jednostka", "")
        data = r.get("Data", "")
        row_html += (
            f"<tr>"
            f"<td style='padding:7px 14px;border-bottom:1px solid #f0f0f0;color:#444;"
            f"font-size:13px;'>{metryka}</td>"
            f"<td style='padding:7px 14px;border-bottom:1px solid #f0f0f0;text-align:right;"
            f"font-weight:bold;color:#111;font-size:13px;'>{wartosc}"
            f"<span style='color:#999;font-size:11px;margin-left:4px;'>{jednostka}</span></td>"
            f"<td style='padding:7px 14px;border-bottom:1px solid #f0f0f0;text-align:right;"
            f"color:#999;font-size:11px;'>{data}</td>"
            f"</tr>"
        )
    return (
        f"<div style='margin-bottom:18px;border-radius:8px;overflow:hidden;"
        f"box-shadow:0 1px 4px rgba(0,0,0,.1);'>"
        f"<div style='background:{color};padding:10px 16px;'>"
        f"<span style='color:#fff;font-size:15px;font-weight:bold;'>"
        f"{icon}&nbsp; {section}</span></div>"
        f"<table style='width:100%;border-collapse:collapse;background:#fff;'>"
        f"<thead><tr style='background:#f7f7f7;'>"
        f"<th style='padding:5px 14px;text-align:left;font-size:11px;color:#888;"
        f"border-bottom:2px solid #eee;font-weight:600;'>Metryka</th>"
        f"<th style='padding:5px 14px;text-align:right;font-size:11px;color:#888;"
        f"border-bottom:2px solid #eee;font-weight:600;'>Warto&#347;&#263;</th>"
        f"<th style='padding:5px 14px;text-align:right;font-size:11px;color:#888;"
        f"border-bottom:2px solid #eee;font-weight:600;'>Data</th>"
        f"</tr></thead>"
        f"<tbody>{row_html}</tbody>"
        f"</table></div>"
    )


def _build_html_body(
    report_df: pd.DataFrame,
    fetch_time: datetime,
    history_sheets: dict[str, pd.DataFrame] | None = None,
) -> str:
    date_str = fetch_time.strftime("%d.%m.%Y %H:%M")
    if history_sheets is None:
        history_sheets = {}

    cards_html = ""
    if not report_df.empty:
        for section in _SECTION_ORDER:
            section_rows = report_df[report_df["Sekcja"] == section]
            if not section_rows.empty:
                cards_html += _build_section_card(section, section_rows)
                cards_html += _build_section_charts(section, history_sheets)
    else:
        cards_html = "<p style='color:#888;'>Brak danych w raporcie.</p>"

    return f"""<!DOCTYPE html>
<html lang="pl">
<head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#f0f4f8;font-family:Arial,Helvetica,sans-serif;color:#333;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f0f4f8;">
    <tr><td align="center" style="padding:24px 12px;">
      <table width="620" cellpadding="0" cellspacing="0"
             style="max-width:620px;width:100%;border-radius:10px;overflow:hidden;
                    box-shadow:0 2px 12px rgba(0,0,0,.12);">

        <!-- Nagłówek -->
        <tr>
          <td style="background:#1F4E79;padding:22px 28px;">
            <div style="color:#fff;font-size:20px;font-weight:bold;margin-bottom:4px;">
              TGE &#8212; Raport danych gie&#322;dowych
            </div>
            <div style="color:#BDD7EE;font-size:13px;">
              Data pobrania: {date_str}
            </div>
          </td>
        </tr>

        <!-- Dashboard -->
        <tr>
          <td style="background:#f0f4f8;padding:20px 16px;">
            {cards_html}
            <p style="font-size:11px;color:#bbb;text-align:center;margin-top:20px;margin-bottom:0;">
              Wiadomo&#347;&#263; wygenerowana automatycznie przez TGE Data Scraper.
              Pe&#322;ne dane historyczne w za&#322;&#261;czniku Excel.
            </p>
          </td>
        </tr>

      </table>
    </td></tr>
  </table>
</body>
</html>"""


def _read_report_df(excel_path: Path) -> pd.DataFrame:
    try:
        xf = pd.ExcelFile(str(excel_path), engine="openpyxl")
        if "Raport_dzienny" in xf.sheet_names:
            df = xf.parse("Raport_dzienny")
            xf.close()
            return df
        xf.close()
    except Exception as exc:
        logger.warning("Nie udalo sie odczytac Raport_dzienny z %s: %s", excel_path, exc)
    return pd.DataFrame()


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_recipients(raw_recipients: object) -> tuple[list[str], list[str]]:
    if isinstance(raw_recipients, str):
        candidates = [item.strip() for item in raw_recipients.replace(";", ",").split(",")]
    elif isinstance(raw_recipients, list):
        candidates = [str(item).strip() for item in raw_recipients]
    else:
        candidates = []

    recipients: list[str] = []
    skipped: list[str] = []
    for recipient in candidates:
        if not recipient:
            continue
        domain = recipient.rsplit("@", 1)[-1].lower() if "@" in recipient else ""
        if domain in PLACEHOLDER_RECIPIENT_DOMAINS:
            skipped.append(recipient)
            continue
        recipients.append(recipient)

    return recipients, skipped


def send_report(
    excel_path: Path,
    summary: str,
    config: dict,
    fetch_time: datetime | None = None,
) -> tuple[bool, str]:
    """
    Send the Excel report by email to recipients from config.
    Returns a tuple: (success, error_message).
    """
    email_cfg = config.get("email", {})
    smtp_server = _clean_text(email_cfg.get("smtp_server", "smtp.gmail.com"))
    smtp_port = int(email_cfg.get("smtp_port", 587))
    sender = _clean_text(email_cfg.get("sender_email", ""))
    password = _clean_text(email_cfg.get("sender_password", ""))
    recipients, skipped_recipients = _normalize_recipients(email_cfg.get("recipients", []))
    subject = _clean_text(email_cfg.get("subject", "TGE - Codzienny raport rynkowy"))
    attach_excel = email_cfg.get("attach_excel", True)

    if not sender or not password:
        message = "Brak danych logowania do SMTP (sender_email / sender_password)."
        logger.error(message)
        return False, message

    if not recipients:
        message = "Brak poprawnych odbiorcow e-mail (email.recipients)."
        logger.error(message)
        return False, message

    if fetch_time is None:
        fetch_time = datetime.now()

    if skipped_recipients:
        logger.warning("Pomijam placeholdery odbiorcow: %s", ", ".join(skipped_recipients))

    report_df = _read_report_df(excel_path)
    history_sheets = _read_history_sheets(excel_path)

    msg = MIMEMultipart("mixed")
    msg["Subject"] = f"{subject} - {fetch_time.strftime('%d.%m.%Y')}"
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)

    html_body = _build_html_body(report_df, fetch_time, history_sheets)
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    if attach_excel and excel_path.exists():
        try:
            with open(excel_path, "rb") as file_handle:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(file_handle.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f'attachment; filename="{excel_path.name}"')
            msg.attach(part)
        except Exception as exc:
            logger.error("Blad przy dolaczaniu pliku Excel: %s", exc)
    elif attach_excel:
        logger.warning("Plik Excel nie istnieje, pomijam zalacznik.")

    try:
        logger.info("Wysylam e-mail przez %s:%d do: %s", smtp_server, smtp_port, ", ".join(recipients))
        with smtplib.SMTP(smtp_server, smtp_port, timeout=30) as server:
            server.ehlo()
            server.starttls(context=ssl.create_default_context())
            server.ehlo()
            server.login(sender, password)
            server.sendmail(sender, recipients, msg.as_bytes())

        logger.info("E-mail wyslany pomyslnie do %d odbiorcow.", len(recipients))
        return True, ""

    except smtplib.SMTPAuthenticationError as exc:
        smtp_error = exc.smtp_error.decode("utf-8", errors="replace") if isinstance(exc.smtp_error, bytes) else str(exc.smtp_error)
        message = f"Blad uwierzytelnienia SMTP ({exc.smtp_code}): {smtp_error}"
        logger.error(message)
        return False, message
    except smtplib.SMTPRecipientsRefused as exc:
        message = f"Serwer odrzucil odbiorcow: {', '.join(exc.recipients.keys())}"
        logger.error(message)
        return False, message
    except smtplib.SMTPException as exc:
        message = f"Blad SMTP: {exc}"
        logger.error(message)
        return False, message
    except Exception as exc:
        message = f"Nieoczekiwany blad wysylki: {exc}"
        logger.error(message)
        return False, message
