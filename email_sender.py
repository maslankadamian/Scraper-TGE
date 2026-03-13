"""
Moduł wysyłki e-mail z raportem Excel TGE.
Obsługuje SMTP z TLS (Gmail, Outlook, itp.).
"""
import logging
import smtplib
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)


def _build_html_body(summary: str, fetch_time: datetime) -> str:
    """Buduje treść HTML e-maila."""
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

    html = f"""
<!DOCTYPE html>
<html lang="pl">
<head><meta charset="UTF-8"></head>
<body style="font-family:Arial,sans-serif;color:#333;max-width:600px;margin:auto;">
  <div style="background:#1F4E79;padding:16px 24px;border-radius:4px 4px 0 0;">
    <h2 style="color:#fff;margin:0;">TGE – Raport danych giełdowych</h2>
    <p style="color:#BDD7EE;margin:4px 0 0;">Data pobrania: {date_str}</p>
  </div>
  <div style="border:1px solid #ddd;border-top:none;padding:16px 24px;">
    <p>W załączniku znajdziesz plik Excel z aktualnymi danymi z TGE
       (Towarowa Giełda Energii), uzupełniony o historyczne rekordy.</p>
    <h3 style="color:#1F4E79;">Podsumowanie zawartości pliku:</h3>
    <table style="border-collapse:collapse;width:100%;">
      {rows}
    </table>
    <p style="margin-top:16px;font-size:12px;color:#888;">
      Źródła danych: <a href="https://tge.pl/">tge.pl</a>,
      <a href="https://tge.pl/otf">tge.pl/otf</a><br>
      Wiadomość wygenerowana automatycznie przez TGE Data Scraper.
    </p>
  </div>
</body>
</html>
"""
    return html


def send_report(
    excel_path: Path,
    summary: str,
    config: dict,
    fetch_time: datetime | None = None,
) -> bool:
    """
    Wysyła e-mail z raportem Excel do wszystkich odbiorców z konfiguracji.
    Zwraca True jeśli wysyłka się powiodła.
    """
    email_cfg = config.get("email", {})
    smtp_server = email_cfg.get("smtp_server", "smtp.gmail.com")
    smtp_port = int(email_cfg.get("smtp_port", 587))
    sender = email_cfg.get("sender_email", "")
    password = email_cfg.get("sender_password", "")
    recipients: list[str] = email_cfg.get("recipients", [])
    subject = email_cfg.get("subject", "TGE – Raport danych giełdowych")
    attach_excel = email_cfg.get("attach_excel", True)

    if not sender or not password:
        logger.error("Brak danych logowania do SMTP (sender_email / sender_password).")
        return False
    if not recipients:
        logger.error("Brak odbiorców e-mail (email.recipients).")
        return False

    if fetch_time is None:
        fetch_time = datetime.now()

    # Buduj wiadomość
    msg = MIMEMultipart("mixed")
    msg["Subject"] = f"{subject} – {fetch_time.strftime('%d.%m.%Y')}"
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)

    # Treść HTML
    html_body = _build_html_body(summary, fetch_time)
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    # Załącznik Excel
    if attach_excel and excel_path.exists():
        try:
            with open(excel_path, "rb") as f:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                f'attachment; filename="{excel_path.name}"',
            )
            msg.attach(part)
            logger.debug("Dołączono załącznik: %s", excel_path.name)
        except Exception as exc:
            logger.error("Błąd przy dołączaniu pliku Excel: %s", exc)
    elif attach_excel:
        logger.warning("Plik Excel nie istnieje, pomijam załącznik.")

    # Wyślij
    try:
        logger.info(
            "Wysyłam e-mail przez %s:%d do: %s",
            smtp_server, smtp_port, ", ".join(recipients),
        )
        with smtplib.SMTP(smtp_server, smtp_port, timeout=30) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(sender, password)
            server.sendmail(sender, recipients, msg.as_bytes())

        logger.info("E-mail wysłany pomyślnie do %d odbiorców.", len(recipients))
        return True

    except smtplib.SMTPAuthenticationError:
        logger.error(
            "Błąd uwierzytelnienia SMTP. Sprawdź sender_email i sender_password. "
            "Dla Gmail użyj App Password: https://myaccount.google.com/apppasswords"
        )
    except smtplib.SMTPException as exc:
        logger.error("Błąd SMTP: %s", exc)
    except Exception as exc:
        logger.error("Nieoczekiwany błąd wysyłki: %s", exc)

    return False
