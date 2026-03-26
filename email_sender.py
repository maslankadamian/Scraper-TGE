"""
Email delivery for the Excel market report.
Supports SMTP with STARTTLS for Gmail, Outlook, and similar providers.
"""
from __future__ import annotations

import base64
import io
import logging
import smtplib
import ssl
from datetime import datetime, timedelta
from email import encoders
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
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

# Metrics to include in the email summary (per section)
_EMAIL_METRICS = {
    "CO2": {"Cena biezaca", "Srednia 7D", "Srednia 30D"},
    "Energia BASE": {"2027", "2028"},
    "Spot energia": {"Srednia dnia", "Srednia 7D", "Srednia 30D"},
    "Gaz": {"Cena biezaca", "Srednia 7D", "Srednia 30D"},
}


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


def _render_chart_png(
    dates: list[str], values: list[float], color: str, unit: str, title: str = "",
) -> bytes | None:
    """Render a line chart as PNG bytes using matplotlib."""
    if len(values) < 2:
        return None

    fig, ax = plt.subplots(figsize=(5.6, 1.8), dpi=130)
    fig.patch.set_facecolor("#fafafa")
    ax.set_facecolor("#fafafa")

    ax.plot(dates, values, color=color, linewidth=2, marker="o", markersize=4, zorder=3)
    ax.fill_between(range(len(values)), values, alpha=0.08, color=color)

    # Auto-scale Y axis to highlight trends instead of starting from 0
    v_min, v_max = min(values), max(values)
    v_range = v_max - v_min if v_max != v_min else abs(v_max) * 0.1 or 1.0
    margin = v_range * 0.15
    ax.set_ylim(v_min - margin, v_max + margin)

    ax.set_ylabel(unit, fontsize=8, color="#888")
    ax.tick_params(axis="both", labelsize=7, colors="#888")
    ax.grid(axis="y", linewidth=0.4, color="#ddd")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#ddd")
    ax.spines["bottom"].set_color("#ddd")

    if title:
        ax.set_title(title, fontsize=9, color="#666", loc="left", pad=4)

    for i, v in enumerate(values):
        ax.annotate(f"{v:.2f}", (i, v), textcoords="offset points",
                    xytext=(0, 7), ha="center", fontsize=6, color="#555")

    fig.tight_layout(pad=0.5)
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    buf.seek(0)
    return buf.read()


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


def _build_section_charts(
    section: str,
    history_sheets: dict[str, pd.DataFrame],
    chart_images: list[tuple[str, bytes]],
) -> str:
    """Build chart HTML for a section; appends (cid, png_bytes) to chart_images."""
    color = _SECTION_COLORS.get(section, "#333")
    html = ""

    def _add_chart(dates: list[str], values: list[float], label: str) -> str:
        png = _render_chart_png(dates, values, color, label)
        if png is None:
            return ""
        cid = f"chart_{section.replace(' ', '_')}_{len(chart_images)}"
        chart_images.append((cid, png))
        return (
            f"<div style='margin:4px 16px 12px 16px;text-align:center;'>"
            f"<img src='cid:{cid}' alt='{section} chart' "
            f"style='max-width:100%;border-radius:6px;'/></div>"
        )

    if section == "CO2":
        df = history_sheets.get("CO2", pd.DataFrame())
        dates, values = _last_7_days(df, "Data", "Cena_CO2")
        html += _add_chart(dates, values, "EUR")

    elif section == "Energia BASE":
        df = history_sheets.get("Energia_BASE", pd.DataFrame())
        for year in (2027, 2028):
            col = f"Cena_BASE_{year}_PLN_MWh"
            dates, values = _last_7_days(df, "Data_Raportu", col)
            html += _add_chart(dates, values, f"BASE {year} PLN/MWh")

    elif section == "Spot energia":
        df = history_sheets.get("Spot_energia_srednia", pd.DataFrame())
        dates, values = _last_7_days(df, "Data_Dostawy", "Cena_Srednia_Dzien_PLN_MWh")
        html += _add_chart(dates, values, "PLN/MWh")

    elif section == "Gaz":
        df = history_sheets.get("Gaz", pd.DataFrame())
        dates, values = _last_7_days(df, "Data_Raportu", "Cena_Biezaca_PLN_MWh")
        html += _add_chart(dates, values, "PLN/MWh")

    return html


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
    chart_images: list[tuple[str, bytes]] | None = None,
) -> str:
    date_str = fetch_time.strftime("%d.%m.%Y %H:%M")
    if history_sheets is None:
        history_sheets = {}
    if chart_images is None:
        chart_images = []

    cards_html = ""
    if not report_df.empty:
        for section in _SECTION_ORDER:
            section_rows = report_df[report_df["Sekcja"] == section]
            allowed = _EMAIL_METRICS.get(section)
            if allowed is not None:
                section_rows = section_rows[section_rows["Metryka"].isin(allowed)]
            if not section_rows.empty:
                cards_html += _build_section_card(section, section_rows)
                cards_html += _build_section_charts(section, history_sheets, chart_images)
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

    chart_images: list[tuple[str, bytes]] = []
    html_body = _build_html_body(report_df, fetch_time, history_sheets, chart_images)

    html_related = MIMEMultipart("related")
    html_related.attach(MIMEText(html_body, "html", "utf-8"))
    for cid, png_data in chart_images:
        img_part = MIMEImage(png_data, _subtype="png")
        img_part.add_header("Content-ID", f"<{cid}>")
        img_part.add_header("Content-Disposition", "inline")
        html_related.attach(img_part)
    msg.attach(html_related)

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
