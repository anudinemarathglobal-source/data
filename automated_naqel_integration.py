import sys
import time
import re
import random
import logging
from datetime import datetime
from functools import wraps

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread.exceptions import WorksheetNotFound, APIError
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# ── LOGGING SETUP ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("naqel_tracker.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ── CONFIG ────────────────────────────────────────────────────
JSON_KEYFILE    = r"C:\Users\USER\Downloads\Emarath_global\service_account.json"
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1hClSA4u_gE5KUudt2Vz2dHJVmh9a3AciCjycjB3Rvds/edit?pli=1&gid=2072052048#gid=2072052048"
TARGETSHEET_URL = "https://docs.google.com/spreadsheets/d/1S6WaI1rXuf5ogN44ph8weA_z_bMCg85U3uaZ7RHvpfM/edit?gid=221183959#gid=221183959"
TRACKING_URL    = "https://new.naqelksa.com/en/ae/tracking/"
BATCH_SIZE      = 25
PAGE_WAIT       = 20
RESULT_WAIT     = 20
SLEEP_INTERVAL  = 3600          # seconds between full runs (1 hour)
SHEETS_DELAY    = 1.2           # seconds between every Sheets API call
MAX_RETRIES     = 6             # max retry attempts for rate-limited calls
# ─────────────────────────────────────────────────────────────


# ── RATE-LIMIT SAFE DECORATOR ─────────────────────────────────
def sheets_call(fn):
    """Wrap any gspread call with exponential backoff + jitter on 429/500."""
    @wraps(fn)
    def wrapper(*args, **kwargs):
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                result = fn(*args, **kwargs)
                time.sleep(SHEETS_DELAY)   # polite gap after every success
                return result
            except APIError as e:
                code = e.response.status_code if hasattr(e, "response") else 0
                if code in (429, 500, 503) and attempt < MAX_RETRIES:
                    wait = min(2 ** attempt + random.uniform(0, 1), 120)
                    log.warning(
                        f"Sheets API error {code} on '{fn.__name__}' "
                        f"(attempt {attempt}/{MAX_RETRIES}). "
                        f"Retrying in {wait:.1f}s…"
                    )
                    time.sleep(wait)
                else:
                    log.error(f"Sheets API failed after {attempt} attempt(s): {e}")
                    raise
            except Exception as e:
                log.error(f"Unexpected error in '{fn.__name__}': {e}")
                raise
    return wrapper


# ── GOOGLE SHEETS HELPERS ─────────────────────────────────────
def _authorize():
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEYFILE, scope)
    return gspread.authorize(creds)


@sheets_call
def _open_by_url(client, url):
    return client.open_by_url(url)


@sheets_call
def _get_worksheet(doc, name):
    return doc.worksheet(name)


@sheets_call
def _add_worksheet(doc, title, rows, cols):
    return doc.add_worksheet(title=title, rows=rows, cols=cols)


@sheets_call
def _sheet_update(sheet, values, range_name):
    sheet.update(values=values, range_name=range_name)


@sheets_call
def _sheet_batch_clear(sheet, ranges):
    sheet.batch_clear(ranges)


@sheets_call
def _sheet_get(sheet, range_name):
    return sheet.get(range_name)


def get_client():
    return _authorize()


def get_google_doc(client):
    return _open_by_url(client, SPREADSHEET_URL)


def get_google_targetdoc(client):
    return _open_by_url(client, TARGETSHEET_URL)


def prepare_output_sheet(doc):
    try:
        sheet = _get_worksheet(doc, "NAQEL_STATUS")
    except WorksheetNotFound:
        sheet = _add_worksheet(doc, "NAQEL_STATUS", rows="2000", cols="10")

    _sheet_update(
        sheet,
        values=[["Tracking ID", "Current Status", "Destination",
                 "Expected Delivery", "Pickup Date", "Last Sync"]],
        range_name="A1:F1",
    )
    _sheet_batch_clear(sheet, ["A2:F2000"])
    return sheet


# ── BROWSER ───────────────────────────────────────────────────
def create_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), options=options
    )
    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    driver.set_page_load_timeout(60)
    return driver


# ── SCRAPING ──────────────────────────────────────────────────
def wait_for_results(driver, timeout=RESULT_WAIT):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            for el in driver.find_elements(By.CSS_SELECTOR, "td, span, div, p"):
                if re.match(r"^\d{8,12}$", el.text.strip()):
                    return True
            body = driver.find_element(By.TAG_NAME, "body").text
            if re.search(r"CURRENT STATUS[:\s]+(.+)", body, re.IGNORECASE):
                return True
        except Exception:
            pass
        time.sleep(1)
    log.warning("Timed out waiting for tracking results.")
    return False


STATUS_KEYWORDS = [
    "Delivered", "In Transit", "Picked Up", "Out for Delivery",
    "Returned", "Cancelled", "Pending",
]


def _extract_fields(segment):
    def get(labels):
        for label in labels:
            m = re.search(
                rf"{re.escape(label)}\s*:?\s*([^\n]+)", segment, re.IGNORECASE
            )
            if m and m.group(1).strip():
                return m.group(1).strip()
        return "N/A"

    status = get(["CURRENT STATUS", "STATUS"])
    if status == "N/A":
        for kw in STATUS_KEYWORDS:
            if kw.lower() in segment.lower():
                status = kw
                break
    return status, get(["DESTINATION"]), get(["EXPECTED DELIVERY"]), get(["PICKUP DATE"])


def parse_results(driver, waybill_ids, current_time):
    body_text = driver.find_element(By.TAG_NAME, "body").text
    rows      = []
    found_ids = set()

    # Primary: split by SHIPMENT NO blocks
    for segment in re.split(r"(?=SHIPMENT NO\s*:\s*\d)", body_text, flags=re.IGNORECASE):
        matched = next((w for w in waybill_ids if str(w) in segment), None)
        if not matched:
            continue
        status, dest, exp_del, pickup = _extract_fields(segment)
        rows.append([str(matched), status, dest, exp_del, pickup, current_time])
        found_ids.add(str(matched))

    # Fallback: raw text scan for IDs not yet found
    for wid in waybill_ids:
        if str(wid) in found_ids:
            continue
        if str(wid) in body_text:
            idx     = body_text.find(str(wid))
            segment = body_text[max(0, idx - 100): idx + 600]
            status, dest, exp_del, pickup = _extract_fields(segment)
        else:
            status, dest, exp_del, pickup = "NOT_FOUND", "N/A", "N/A", "N/A"
        rows.append([str(wid), status, dest, exp_del, pickup, current_time])

    return rows


def scrape_batch(driver, waybill_ids, current_time):
    wait      = WebDriverWait(driver, PAGE_WAIT)
    error_row = lambda reason: [
        [str(w), reason, "N/A", "N/A", "N/A", current_time] for w in waybill_ids
    ]

    # Load page
    try:
        driver.get(TRACKING_URL)
        time.sleep(3)
    except Exception as e:
        log.error(f"Error loading tracking page: {e}")
        return error_row("PAGE_LOAD_ERROR")

    # Fill textarea
    try:
        textarea = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "textarea.form-control"))
        )
        textarea.clear()
        time.sleep(0.3)
        textarea.send_keys(", ".join(str(w) for w in waybill_ids))
        time.sleep(0.3)
    except Exception as e:
        log.error(f"Error filling textarea: {e}")
        return error_row("INPUT_ERROR")

    # Click Track button
    try:
        btn = next(
            (b for b in driver.find_elements(By.CSS_SELECTOR, "button")
             if b.text.strip().lower() == "track" and b.is_displayed()),
            None,
        ) or wait.until(
            EC.element_to_be_clickable(
                (By.CSS_SELECTOR, "button[class*='track'], button.btn-danger")
            )
        )
        driver.execute_script("arguments[0].click();", btn)
    except Exception as e:
        log.error(f"Error clicking Track button: {e}")
        return error_row("BTN_ERROR")

    wait_for_results(driver)
    return parse_results(driver, waybill_ids, current_time)


# ── MAIN ──────────────────────────────────────────────────────
def main():
    log.info("Connecting to Google Sheets…")
    try:
        client       = get_client()
        target_doc   = get_google_targetdoc(client)
        doc          = get_google_doc(client)
        input_sheet  = _get_worksheet(doc, "NAQEL_TRACK_ID")
        output_sheet = prepare_output_sheet(target_doc)
    except Exception as e:
        log.error(f"Google Sheets initialisation failed: {e}")
        sys.exit(1)

    raw_rows  = _sheet_get(input_sheet, "A2:F")
    naqel_ids = [row[0] for row in raw_rows if row and row[0]]

    if not naqel_ids:
        log.warning("No IDs found in NAQEL_TRACK_ID sheet (column A).")
        return

    total_batches = (len(naqel_ids) + BATCH_SIZE - 1) // BATCH_SIZE
    log.info(f"Found {len(naqel_ids)} IDs — {total_batches} batch(es) of up to {BATCH_SIZE}.")
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    log.info("Starting Chrome browser…")
    try:
        driver = create_driver()
    except Exception as e:
        log.error(f"Failed to launch Chrome: {e}")
        sys.exit(1)

    all_rows = []
    try:
        for batch_num, i in enumerate(range(0, len(naqel_ids), BATCH_SIZE), start=1):
            batch = naqel_ids[i: i + BATCH_SIZE]
            log.info(f"Batch {batch_num}/{total_batches} ({len(batch)} IDs)…")
            rows = scrape_batch(driver, batch, current_time)
            all_rows.extend(rows)

            if rows:
                _sheet_update(output_sheet, values=rows, range_name=f"A{i + 2}")
                log.info(f"  Written {len(rows)} row(s) to sheet.")

            if batch_num < total_batches:
                time.sleep(3)

    except KeyboardInterrupt:
        log.info("Stopped by user.")
    finally:
        driver.quit()

    log.info(f"Run complete. Total rows written: {len(all_rows)}")


# ── ENTRY POINT ───────────────────────────────────────────────
if __name__ == "__main__":
    log.info("=== Naqel Auto-Tracker started ===")
    try:
        while True:
            main()
            log.info(f"Sleeping {SLEEP_INTERVAL // 60} minutes until next run…")
            time.sleep(SLEEP_INTERVAL)
    except KeyboardInterrupt:
        log.info("Shutdown requested. Goodbye.")
        sys.exit(0)