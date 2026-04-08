"""
Naqel KSA Shipment Tracking Script — Web Scraping Edition
===========================================================
Uses Selenium to track waybills via new.naqelksa.com/en/ae/tracking/
No API credentials required.

SETUP:
1. pip install gspread oauth2client selenium webdriver-manager
2. Place your Google Service Account JSON keyfile in same directory.
3. Fill in CONFIG below.

COMMANDS:
   python X_naqel_ksa_analysis.py           <- normal run
   python X_naqel_ksa_analysis.py --debug   <- visible browser, 1 ID, prints debug info
"""

import sys
import time
import re

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread.exceptions import WorksheetNotFound

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
from webdriver_manager.chrome import ChromeDriverManager

# ─────────────────────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────────────────────
JSON_KEYFILE    = r"C:\Users\USER\Downloads\Emarath_global\service_account.json"
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1eo2tY_57lcOTtGOyU5GOz2IQON3b0VsVf8rTsbfT3HM/edit?gid=1033736307#gid=1033736307"
TRACKING_URL    = "https://new.naqelksa.com/en/ae/tracking/"

BATCH_SIZE      = 25     # IDs per browser session
PAGE_WAIT       = 20    # Seconds to wait for page elements
RESULT_WAIT     = 20    # Seconds to wait for AJAX results after clicking Track
HEADLESS        = True
# ─────────────────────────────────────────────────────────────


# ── Google Sheets ─────────────────────────────────────────────────────────────

def get_google_doc():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds  = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEYFILE, scope)
    client = gspread.authorize(creds)
    return client.open_by_url(SPREADSHEET_URL)


def prepare_output_sheet(doc):
    try:
        sheet = doc.worksheet("NAQEL_STATUS")
    except WorksheetNotFound:
        sheet = doc.add_worksheet(title="NAQEL_STATUS", rows="2000", cols="10")
    headers = [["Tracking ID", "Current Status", "Destination",
                "Expected Delivery", "Pickup Date", "Last Sync"]]
    sheet.update(values=headers, range_name="A1:F1")
    sheet.batch_clear(["A2:F2000"])
    return sheet


# ── Browser ───────────────────────────────────────────────────────────────────

def create_driver(headless=True):
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-extensions")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )
    service = Service(ChromeDriverManager().install())
    driver  = webdriver.Chrome(service=service, options=options)
    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    driver.set_page_load_timeout(60)
    return driver


# ── Wait for results to actually populate ─────────────────────────────────────

def wait_for_results_populated(driver, timeout=RESULT_WAIT):
    """
    Poll until SHIPMENT NO field has a real value (not empty).
    The page renders fields as empty labels first, then AJAX fills them in.
    """
    print(f"  Waiting up to {timeout}s for AJAX data to populate...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            # Look for any element near "SHIPMENT NO" that has actual numeric content
            # The page structure: label span + value span side by side
            elements = driver.find_elements(By.CSS_SELECTOR, "td, span, div, p")
            for el in elements:
                text = el.text.strip()
                # A real shipment number is all digits, 8-12 chars
                if re.match(r"^\d{8,12}$", text):
                    print(f"  Data populated! Found shipment number: {text}")
                    return True
            # Also check if CURRENT STATUS has a value
            body_text = driver.find_element(By.TAG_NAME, "body").text
            # If status labels have values after them (not just labels)
            if re.search(r"CURRENT STATUS[:\s]+\S", body_text, re.IGNORECASE):
                status_match = re.search(r"CURRENT STATUS[:\s]+(.+)", body_text, re.IGNORECASE)
                if status_match:
                    val = status_match.group(1).strip().split("\n")[0]
                    if val and val.upper() not in ("CURRENT STATUS", "STATUS", "N/A"):
                        print(f"  Data populated! Status: {val}")
                        return True
        except Exception:
            pass
        time.sleep(1)

    print("  WARNING: Timed out waiting for data. Page may be slow or waybill not found.")
    return False


# ── Parse results from page ───────────────────────────────────────────────────


def scrape_all_results(driver, waybill_ids, current_time, is_debug=False):
    """
    Parse tracking results from full page body text.
    The page text contains blocks like:
      SHIPMENT NO: 397713669
      DESTINATION: JEDDAH
      EXPECTED DELIVERY: 10-02-2026
      PICKUP DATE: 05-02-2026
      PAYMENT METHOD: Payment on delivery
      PIECE COUNT: 1
      CURRENT STATUS: Delivered
    We split the page into per-waybill segments and extract each field.
    """
    body_text = driver.find_element(By.TAG_NAME, "body").text

    if is_debug:
        print("\n  === Full page body text ===")
        print(body_text)

    rows      = []
    found_ids = set()

    # ── Split page text into per-shipment segments ────────────────────────────
    # Anchor: each result block starts at "SHIPMENT NO: <number>"
    # Split on that pattern to get one segment per result
    segments = re.split(r"(?=SHIPMENT NO\s*:\s*\d)", body_text, flags=re.IGNORECASE)

    if is_debug:
        print(f"\n  Found {len(segments)} segment(s) starting with 'SHIPMENT NO:'")

    for segment in segments:
        # Check if this segment contains one of our waybill IDs
        matched_wid = None
        for wid in waybill_ids:
            if str(wid) in segment:
                matched_wid = str(wid)
                break
        if not matched_wid:
            continue

        if is_debug:
            print(f"\n  Segment for {matched_wid}:\n{segment[:400]}")

        def get(labels):
            for label in labels:
                m = re.search(
                    rf"{re.escape(label)}\s*:?\s*([^\n]+)",
                    segment,
                    re.IGNORECASE
                )
                if m:
                    val = m.group(1).strip()
                    if val:
                        return val
            return "N/A"

        status  = get(["CURRENT STATUS", "STATUS"])
        dest    = get(["DESTINATION"])
        exp_del = get(["EXPECTED DELIVERY"])
        pickup  = get(["PICKUP DATE"])

        # Keyword fallback for status
        if status == "N/A":
            for kw in ["Delivered", "In Transit", "Picked Up", "Out for Delivery",
                       "Returned", "Cancelled", "Pending"]:
                if kw.lower() in segment.lower():
                    status = kw
                    break

        rows.append([matched_wid, status, dest, exp_del, pickup, current_time])
        found_ids.add(matched_wid)

    # ── Fallback: simple full-page search for each ID not yet found ───────────
    if not rows or len(found_ids) < len(waybill_ids):
        for wid in waybill_ids:
            if str(wid) in found_ids:
                continue
            if str(wid) in body_text:
                # ID is on page but no SHIPMENT NO block — try raw extraction
                idx     = body_text.find(str(wid))
                segment = body_text[max(0, idx - 100): idx + 600]

                def get_fallback(labels):
                    for label in labels:
                        m = re.search(rf"{re.escape(label)}\s*:?\s*([^\n]+)", segment, re.IGNORECASE)
                        if m:
                            val = m.group(1).strip()
                            if val:
                                return val
                    return "N/A"

                status  = get_fallback(["CURRENT STATUS", "STATUS"])
                dest    = get_fallback(["DESTINATION"])
                exp_del = get_fallback(["EXPECTED DELIVERY"])
                pickup  = get_fallback(["PICKUP DATE"])

                if status == "N/A":
                    for kw in ["Delivered", "In Transit", "Picked Up", "Out for Delivery",
                               "Returned", "Cancelled", "Pending"]:
                        if kw.lower() in segment.lower():
                            status = kw
                            break

                rows.append([str(wid), status, dest, exp_del, pickup, current_time])
                found_ids.add(str(wid))
            else:
                rows.append([str(wid), "NOT_FOUND", "N/A", "N/A", "N/A", current_time])

    if is_debug:
        print(f"\n  Found data blocks for waybill IDs: {list(found_ids)}")

    return rows


# ── Scrape one batch ──────────────────────────────────────────────────────────

def scrape_batch(driver, waybill_ids, current_time, is_debug=False):
    wait = WebDriverWait(driver, PAGE_WAIT)

    # Load page
    try:
        driver.get(TRACKING_URL)
        time.sleep(3)
    except Exception as e:
        print(f"  ERROR loading page: {e}")
        return [[str(w), "PAGE_LOAD_ERROR", "N/A", "N/A", "N/A", current_time] for w in waybill_ids]

    # Find textarea — we know from debug it's: textarea.form-control.form-control-sm
    try:
        textarea = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "textarea.form-control"))
        )
        textarea.clear()
        time.sleep(0.3)
        textarea.send_keys(", ".join(str(w) for w in waybill_ids))
        time.sleep(0.3)
        print(f"  Entered {len(waybill_ids)} IDs into textarea.")
    except Exception as e:
        print(f"  ERROR filling textarea: {e}")
        return [[str(w), "INPUT_ERROR", "N/A", "N/A", "N/A", current_time] for w in waybill_ids]

    # Click Track — we know it has class 'btn-circle-track' from debug
    try:
        track_btn = wait.until(
            EC.element_to_be_clickable(
                (By.CSS_SELECTOR, "button[class*='track'], button.btn-danger")
            )
        )
        # Make sure we're clicking the Track submission button, not the 1/2/3 step buttons
        # The step buttons also have btn-circle-track — find the one in the form
        all_btns = driver.find_elements(By.CSS_SELECTOR, "button")
        form_track_btn = None
        for btn in all_btns:
            txt = btn.text.strip()
            if txt.lower() == "track" and btn.is_displayed():
                form_track_btn = btn
                break
        btn_to_click = form_track_btn or track_btn
        driver.execute_script("arguments[0].click();", btn_to_click)
        print(f"  Clicked Track button.")
    except Exception as e:
        print(f"  ERROR clicking Track: {e}")
        return [[str(w), "BTN_ERROR", "N/A", "N/A", "N/A", current_time] for w in waybill_ids]

    # Wait for AJAX data to populate
    wait_for_results_populated(driver, timeout=RESULT_WAIT)

    # Scrape
    return scrape_all_results(driver, waybill_ids, current_time, is_debug=is_debug)


# ── Main ──────────────────────────────────────────────────────────────────────

def main(is_debug=False):
    print("Connecting to Google Sheets...")
    try:
        doc          = get_google_doc()
        input_sheet  = doc.worksheet("TRACK_DATA")
        output_sheet = prepare_output_sheet(doc)
    except Exception as e:
        print(f"Google Sheets error: {e}")
        sys.exit(1)

    all_data  = input_sheet.get("A2:F")
    naqel_ids = [
        row[0]
        for row in all_data
        if len(row) >= 6
        and str(row[5]).strip().upper() == "NAQEL"
        and row[0]
    ]

    if not naqel_ids:
        print("No NAQEL records found in TRACK_DATA (Column F must say 'NAQEL').")
        return

    total_batches = (len(naqel_ids) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"Found {len(naqel_ids)} Naqel IDs — {total_batches} batches of {BATCH_SIZE}.")
    current_time = time.strftime("%Y-%m-%d %H:%M:%S")

    print("Starting browser...")
    try:
        driver = create_driver(headless=HEADLESS and not is_debug)
    except Exception as e:
        print(f"Failed to launch Chrome: {e}")
        sys.exit(1)

    all_rows = []
    try:
        if is_debug:
            print(f"\n=== DEBUG MODE — Testing first ID: {naqel_ids[0]} ===")
            rows = scrape_batch(driver, [naqel_ids[0]], current_time, is_debug=True)
            print("\n=== Scraped rows ===")
            for row in rows:
                print(row)
            input("\nPress Enter to close browser...")
            return

        for batch_num, i in enumerate(range(0, len(naqel_ids), BATCH_SIZE), start=1):
            batch = naqel_ids[i:i + BATCH_SIZE]
            print(f"\nBatch {batch_num}/{total_batches} ({len(batch)} IDs)...")

            rows = scrape_batch(driver, batch, current_time)
            all_rows.extend(rows)

            if rows:
                start_row = i + 2
                output_sheet.update(values=rows, range_name=f"A{start_row}")
                print(f"  Wrote {len(rows)} rows to sheet (row {start_row} onward).")

            if batch_num < total_batches:
                time.sleep(3)

    except KeyboardInterrupt:
        print("\n[STOPPED]")
    finally:
        driver.quit()

    print(f"\nDone. Total rows written: {len(all_rows)}")


# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    is_debug = "--debug" in sys.argv
    main(is_debug=is_debug)