"""
TGE Data Scraper – aplikacja webowa (Flask).

Uruchomienie:
  python app.py
  Następnie otwórz: http://localhost:5000
"""
import logging
import sys
import threading
from datetime import datetime
from pathlib import Path

import yaml
from flask import Flask, Response, jsonify, render_template_string, send_file

from data_manager import get_summary
from email_sender import send_report
from main import load_config, run_cycle, setup_schedule

import schedule
import time

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

# ── Stan aplikacji ────────────────────────────────────────────────────────────
_state = {
    "status": "idle",           # idle | running | done | error
    "message": "",
    "last_run": None,
    "last_summary": "",
    "excel_path": None,
    "lock": threading.Lock(),
}

app = Flask(__name__)
_config: dict = {}


# ── HTML Template ─────────────────────────────────────────────────────────────
TEMPLATE = """
<!DOCTYPE html>
<html lang="pl">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>TGE Data Scraper</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
  <link href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.css" rel="stylesheet">
  <style>
    body { background: #f0f4f8; }
    .navbar { background: #1F4E79 !important; }
    .card { border: none; border-radius: 12px; box-shadow: 0 2px 8px rgba(0,0,0,.08); }
    .card-header { border-radius: 12px 12px 0 0 !important; font-weight: 600; }
    .btn-scrape  { background: #1F4E79; border: none; font-size: 1.1rem; padding: .6rem 2rem; }
    .btn-scrape:hover  { background: #163a5c; }
    .btn-scrape:disabled { background: #7a9ab8; }
    .btn-dl { background: #217346; border: none; }
    .btn-dl:hover { background: #185a37; }
    .btn-mail { background: #c55a11; border: none; }
    .btn-mail:hover { background: #9b460d; }
    #log-box {
      background: #1a1a2e; color: #a8d8a8; font-family: monospace;
      font-size: .83rem; height: 240px; overflow-y: auto;
      border-radius: 8px; padding: 12px;
    }
    .badge-idle    { background: #6c757d; }
    .badge-running { background: #ffc107; color: #000; }
    .badge-done    { background: #198754; }
    .badge-error   { background: #dc3545; }
    .spinner-border-sm { width: 1rem; height: 1rem; }
    .schedule-info { font-size: .9rem; color: #555; }
  </style>
</head>
<body>

<nav class="navbar navbar-dark px-4 py-2">
  <span class="navbar-brand fw-bold fs-5">
    <i class="bi bi-lightning-charge-fill me-2"></i>TGE Data Scraper
  </span>
  <span class="text-white-50 small">Towarowa Giełda Energii</span>
</nav>

<div class="container py-4">
  <div class="row g-4">

    <!-- Lewa kolumna: sterowanie -->
    <div class="col-lg-5">

      <!-- Status -->
      <div class="card mb-4">
        <div class="card-header bg-primary text-white">
          <i class="bi bi-activity me-2"></i>Status
        </div>
        <div class="card-body">
          <div class="d-flex align-items-center gap-2 mb-3">
            <span class="fw-semibold">Stan:</span>
            <span id="status-badge" class="badge badge-idle px-3 py-2">Gotowy</span>
            <span id="spinner" class="spinner-border spinner-border-sm text-warning d-none" role="status"></span>
          </div>
          <div id="status-msg" class="text-muted small mb-3"></div>
          <div class="schedule-info">
            <i class="bi bi-clock me-1"></i>
            <strong>Harmonogram:</strong> {{ schedule_info }}
          </div>
          {% if last_run %}
          <div class="schedule-info mt-1">
            <i class="bi bi-calendar-check me-1"></i>
            <strong>Ostatnie pobranie:</strong> {{ last_run }}
          </div>
          {% endif %}
        </div>
      </div>

      <!-- Akcje -->
      <div class="card mb-4">
        <div class="card-header bg-dark text-white">
          <i class="bi bi-play-circle me-2"></i>Akcje
        </div>
        <div class="card-body d-grid gap-3">

          <button id="btn-scrape" class="btn btn-scrape btn-lg text-white w-100"
                  onclick="triggerScrape()">
            <i class="bi bi-cloud-download me-2"></i>Pobierz dane z TGE teraz
          </button>

          <button id="btn-download" class="btn btn-dl btn-lg text-white w-100"
                  onclick="downloadExcel()" {{ '' if excel_exists else 'disabled' }}>
            <i class="bi bi-file-earmark-excel me-2"></i>Pobierz raport Excel
          </button>

          <button id="btn-email" class="btn btn-mail text-white w-100"
                  onclick="sendEmail()" {{ '' if excel_exists else 'disabled' }}>
            <i class="bi bi-envelope me-2"></i>Wyślij raport e-mailem
          </button>

        </div>
      </div>

      <!-- Źródła -->
      <div class="card">
        <div class="card-header bg-secondary text-white">
          <i class="bi bi-link-45deg me-2"></i>Źródła danych
        </div>
        <div class="card-body">
          {% for url in urls %}
          <div><i class="bi bi-arrow-right-short text-primary"></i>
            <a href="{{ url }}" target="_blank">{{ url }}</a></div>
          {% endfor %}
        </div>
      </div>

    </div>

    <!-- Prawa kolumna: podsumowanie + log -->
    <div class="col-lg-7">

      <!-- Podsumowanie pliku -->
      <div class="card mb-4">
        <div class="card-header bg-success text-white">
          <i class="bi bi-table me-2"></i>Zawartość pliku Excel
        </div>
        <div class="card-body">
          <pre id="summary" class="mb-0 text-muted" style="font-size:.9rem;white-space:pre-wrap;">{{ summary or 'Brak danych – uruchom pierwsze pobranie.' }}</pre>
        </div>
      </div>

      <!-- Log -->
      <div class="card">
        <div class="card-header bg-dark text-white d-flex justify-content-between align-items-center">
          <span><i class="bi bi-terminal me-2"></i>Log operacji</span>
          <button class="btn btn-sm btn-outline-light py-0" onclick="clearLog()">Wyczyść</button>
        </div>
        <div class="card-body p-0">
          <div id="log-box"></div>
        </div>
      </div>

    </div>
  </div>
</div>

<script>
  let polling = null;

  function log(msg, cls='text-success') {
    const box = document.getElementById('log-box');
    const ts = new Date().toLocaleTimeString('pl-PL');
    box.innerHTML += `<div class="${cls}">[${ts}] ${msg}</div>`;
    box.scrollTop = box.scrollHeight;
  }

  function clearLog() {
    document.getElementById('log-box').innerHTML = '';
  }

  function setStatus(status, msg) {
    const badge = document.getElementById('status-badge');
    const spinner = document.getElementById('spinner');
    const msgEl = document.getElementById('status-msg');
    const classes = {
      idle: ['badge-idle', 'Gotowy'],
      running: ['badge-running', 'Pobieranie...'],
      done: ['badge-done', 'Zakończono'],
      error: ['badge-error', 'Błąd'],
    };
    badge.className = 'badge px-3 py-2 ' + (classes[status]?.[0] ?? 'badge-idle');
    badge.textContent = classes[status]?.[1] ?? status;
    spinner.classList.toggle('d-none', status !== 'running');
    msgEl.textContent = msg || '';
  }

  function setBusy(busy) {
    document.getElementById('btn-scrape').disabled = busy;
    document.getElementById('btn-email').disabled = busy;
  }

  function triggerScrape() {
    log('Wysyłam żądanie pobrania danych...', 'text-warning');
    setBusy(true);
    setStatus('running', 'Łączę z TGE...');

    fetch('/api/scrape', {method: 'POST'})
      .then(r => r.json())
      .then(data => {
        if (data.started) {
          log('Scraping uruchomiony w tle.', 'text-info');
          polling = setInterval(pollStatus, 2000);
        } else {
          log('Błąd uruchomienia: ' + data.error, 'text-danger');
          setStatus('error', data.error);
          setBusy(false);
        }
      })
      .catch(e => {
        log('Błąd połączenia: ' + e, 'text-danger');
        setStatus('error', '');
        setBusy(false);
      });
  }

  function pollStatus() {
    fetch('/api/status')
      .then(r => r.json())
      .then(data => {
        setStatus(data.status, data.message);
        if (data.status === 'done') {
          clearInterval(polling);
          setBusy(false);
          log('Pobieranie zakończone. Czas: ' + data.elapsed + 's', 'text-success');
          document.getElementById('summary').textContent = data.summary || '';
          document.getElementById('btn-download').disabled = false;
          document.getElementById('btn-email').disabled = false;
        } else if (data.status === 'error') {
          clearInterval(polling);
          setBusy(false);
          log('Błąd: ' + data.message, 'text-danger');
        }
      })
      .catch(() => {});
  }

  function downloadExcel() {
    log('Pobieranie pliku Excel...', 'text-info');
    window.location.href = '/api/download';
  }

  function sendEmail() {
    log('Wysyłam e-mail...', 'text-warning');
    document.getElementById('btn-email').disabled = true;
    fetch('/api/send-email', {method: 'POST'})
      .then(r => r.json())
      .then(data => {
        if (data.ok) {
          log('E-mail wysłany pomyślnie!', 'text-success');
        } else {
          log('Błąd wysyłki: ' + data.error, 'text-danger');
        }
        document.getElementById('btn-email').disabled = false;
      })
      .catch(e => {
        log('Błąd: ' + e, 'text-danger');
        document.getElementById('btn-email').disabled = false;
      });
  }
</script>
</body>
</html>
"""


# ── Pomocnicze ────────────────────────────────────────────────────────────────

def _excel_path() -> Path | None:
    with _state["lock"]:
        p = _state["excel_path"]
    if p and Path(p).exists():
        return Path(p)
    # Spróbuj z konfiguracji
    data_cfg = _config.get("data", {})
    filename = data_cfg.get("output_file", "tge_dane_historyczne.xlsx")
    output_dir = data_cfg.get("output_dir", "") or ""
    candidate = (Path(output_dir) / filename) if output_dir else Path(filename)
    return candidate if candidate.exists() else None


def _schedule_info() -> str:
    sched = _config.get("schedule", {})
    freq = sched.get("frequency", "daily")
    t = sched.get("time", "08:00")
    day = sched.get("day", "monday")
    labels = {
        "daily": f"Codziennie o {t}",
        "weekly": f"Co tydzień ({day}) o {t}",
        "hourly": "Co godzinę",
        "manual": "Tylko ręcznie",
    }
    return labels.get(freq, freq)


# ── Scraping w tle ────────────────────────────────────────────────────────────

def _scrape_thread() -> None:
    with _state["lock"]:
        _state["status"] = "running"
        _state["message"] = "Trwa pobieranie..."

    try:
        result = run_cycle(_config)
    except Exception as exc:
        with _state["lock"]:
            _state["status"] = "error"
            _state["message"] = str(exc)
        logger.exception("Błąd w wątku scrapingu")
        return

    with _state["lock"]:
        if result["ok"]:
            _state["status"] = "done"
            _state["message"] = f"Pobrano w {result['elapsed']:.1f}s"
            _state["last_run"] = result["start"].strftime("%Y-%m-%d %H:%M:%S")
            _state["last_summary"] = result["summary"]
            _state["excel_path"] = str(result["excel_path"])
        else:
            _state["status"] = "error"
            _state["message"] = result.get("error", "Nieznany błąd")


# ── Harmonogram w tle ─────────────────────────────────────────────────────────

def _scheduler_thread() -> None:
    setup_schedule(_config)
    while True:
        schedule.run_pending()
        time.sleep(30)


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    ep = _excel_path()
    with _state["lock"]:
        summary = _state["last_summary"] or (get_summary(ep) if ep else "")
        last_run = _state["last_run"]

    return render_template_string(
        TEMPLATE,
        schedule_info=_schedule_info(),
        last_run=last_run,
        summary=summary,
        excel_exists=ep is not None,
        urls=_config.get("scraping", {}).get("urls", []),
    )


@app.route("/api/scrape", methods=["POST"])
def api_scrape():
    with _state["lock"]:
        if _state["status"] == "running":
            return jsonify({"started": False, "error": "Scraping już trwa."})
        _state["status"] = "running"

    t = threading.Thread(target=_scrape_thread, daemon=True)
    t.start()
    return jsonify({"started": True})


@app.route("/api/status")
def api_status():
    with _state["lock"]:
        return jsonify({
            "status": _state["status"],
            "message": _state["message"],
            "last_run": _state["last_run"],
            "summary": _state["last_summary"],
            "elapsed": _state["message"].replace("Pobrano w ", "").replace("s", "")
                       if "Pobrano" in _state["message"] else "",
        })


@app.route("/api/download")
def api_download():
    ep = _excel_path()
    if not ep:
        return Response("Brak pliku Excel. Uruchom najpierw pobieranie.", status=404)
    return send_file(
        ep,
        as_attachment=True,
        download_name=ep.name,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.route("/api/send-email", methods=["POST"])
def api_send_email():
    ep = _excel_path()
    if not ep:
        return jsonify({"ok": False, "error": "Brak pliku Excel."})

    with _state["lock"]:
        summary = _state["last_summary"] or get_summary(ep)

    ok, error_message = send_report(ep, summary, _config, fetch_time=datetime.now())
    if ok:
        return jsonify({"ok": True})
    return jsonify({"ok": False, "error": error_message})


# ── Start ─────────────────────────────────────────────────────────────────────

def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="TGE Data Scraper – web UI")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--no-scheduler", action="store_true",
                        help="Wyłącz automatyczny harmonogram (tylko ręczne pobieranie)")
    args = parser.parse_args()

    global _config
    _config = load_config(args.config)

    freq = _config.get("schedule", {}).get("frequency", "daily")
    if not args.no_scheduler and freq != "manual":
        sched_t = threading.Thread(target=_scheduler_thread, daemon=True)
        sched_t.start()
        logger.info("Harmonogram uruchomiony w tle.")

    logger.info("Otwórz przeglądarkę: http://%s:%d", args.host, args.port)
    app.run(host=args.host, port=args.port, debug=False, use_reloader=False)


if __name__ == "__main__":
    main()
