"""
Email delivery for the Excel market report.
Supports SMTP with STARTTLS for Gmail, Outlook, and similar providers.
"""
from __future__ import annotations

import logging
import smtplib
import ssl
from datetime import datetime
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

logger = logging.getLogger(__name__)

PLACEHOLDER_RECIPIENT_DOMAINS = {"example.com", "example.org", "example.net", "localhost"}


def _build_html_body(summary: str, fetch_time: datetime) -> str:
    """Build the HTML body for the outgoing message."""
    date_str = fetch_time.strftime("%d.%m.%Y %H:%M")
    rows = ""
    for line in summary.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("-"):
            rows += f"<tr><td style='padding:4px 12px;color:#555;'>{line[1:].strip()}</td></tr>\n"
        else:
            rows += (
                f"<tr><td style='padding:6px 12px;font-weight:bold;"
                f"background:#EEF4FB;'>{line}</td></tr>\n"
            )

    return f"""
<!DOCTYPE html>
<html lang="pl">
<head><meta charset="UTF-8"></head>
<body style="font-family:Arial,sans-serif;color:#333;max-width:600px;margin:auto;">
  <div style="background:#1F4E79;padding:16px 24px;border-radius:4px 4px 0 0;">
    <h2 style="color:#fff;margin:0;">TGE - Raport danych gieldowych</h2>
    <p style="color:#BDD7EE;margin:4px 0 0;">Data pobrania: {date_str}</p>
  </div>
  <div style="border:1px solid #ddd;border-top:none;padding:16px 24px;">
    <p>W zalaczniku znajdziesz plik Excel z aktualnymi danymi rynkowymi i historia.</p>
    <h3 style="color:#1F4E79;">Podsumowanie zawartosci pliku:</h3>
    <table style="border-collapse:collapse;width:100%;">
      {rows}
    </table>
    <p style="margin-top:16px;font-size:12px;color:#888;">
      Wiadomosc wygenerowana automatycznie przez TGE Data Scraper.
    </p>
  </div>
</body>
</html>
"""


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

    msg = MIMEMultipart("mixed")
    msg["Subject"] = f"{subject} - {fetch_time.strftime('%d.%m.%Y')}"
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)

    html_body = _build_html_body(summary, fetch_time)
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
