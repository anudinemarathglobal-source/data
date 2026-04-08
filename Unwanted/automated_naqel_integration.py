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
from webdriver_manager.chrome import ChromeDriverManager

# ── CONFIG ────────────────────────────────────────────────────
JSON_KEYFILE    = r"C:\Users\USER\Downloads\Emarath_global\service_account.json"
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1hClSA4u_gE5KUudt2Vz2dHJVmh9a3AciCjycjB3Rvds/edit?pli=1&gid=2072052048#gid=2072052048"
TARGETSHEET_URL = "https://docs.google.com/spreadsheets/d/1S6WaI1rXuf5ogN44ph8weA_z_bMCg85U3uaZ7RHvpfM/edit?gid=221183959#gid=221183959"
TRACKING_URL    = "https://new.naqelksa.com/en/ae/tracking/"
BATCH_SIZE      = 25
PAGE_WAIT       = 20
RESULT_WAIT     = 20
# ─────────────────────────────────────────────────────────────


def get_google_doc():
    scope  = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds  = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEYFILE, scope)
    client = gspread.authorize(creds)
    return client.open_by_url(SPREADSHEET_URL)

def get_google_targetdoc():
    scope  = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds  = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEYFILE, scope)
    client = gspread.authorize(creds)
    return client.open_by_url(TARGETSHEET_URL)


def prepare_output_sheet(doc):
    try:
        sheet = doc.worksheet("NAQEL_STATUS")
    except WorksheetNotFound:
        sheet = doc.add_worksheet(title="NAQEL_STATUS", rows="2000", cols="10")
    sheet.update(values=[["Tracking ID", "Current Status", "Destination",
                           "Expected Delivery", "Pickup Date", "Last Sync"]], range_name="A1:F1")
    sheet.batch_clear(["A2:F2000"])
    return sheet


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
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                         "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    driver.set_page_load_timeout(60)
    return driver


def wait_for_results(driver, timeout=RESULT_WAIT):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            for el in driver.find_elements(By.CSS_SELECTOR, "td, span, div, p"):
                if re.match(r"^\d{8,12}$", el.text.strip()):
                    return True
            body = driver.find_element(By.TAG_NAME, "body").text
            match = re.search(r"CURRENT STATUS[:\s]+(.+)", body, re.IGNORECASE)
            if match:
                val = match.group(1).strip().split("\n")[0]
                if val and val.upper() not in ("CURRENT STATUS", "STATUS", "N/A"):
                    return True
        except Exception:
            pass
        time.sleep(1)
    print("  WARNING: Timed out waiting for results.")
    return False


def parse_results(driver, waybill_ids, current_time):
    body_text = driver.find_element(By.TAG_NAME, "body").text
    rows      = []
    found_ids = set()

    STATUS_KEYWORDS = ["Delivered", "In Transit", "Picked Up", "Out for Delivery",
                       "Returned", "Cancelled", "Pending"]

    def extract(segment):
        def get(labels):
            for label in labels:
                m = re.search(rf"{re.escape(label)}\s*:?\s*([^\n]+)", segment, re.IGNORECASE)
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

    # Primary: split by SHIPMENT NO blocks
    for segment in re.split(r"(?=SHIPMENT NO\s*:\s*\d)", body_text, flags=re.IGNORECASE):
        matched = next((w for w in waybill_ids if str(w) in segment), None)
        if not matched:
            continue
        status, dest, exp_del, pickup = extract(segment)
        rows.append([str(matched), status, dest, exp_del, pickup, current_time])
        found_ids.add(str(matched))

    # Fallback: search raw text for any IDs not yet found
    for wid in waybill_ids:
        if str(wid) in found_ids:
            continue
        if str(wid) in body_text:
            idx     = body_text.find(str(wid))
            segment = body_text[max(0, idx - 100): idx + 600]
            status, dest, exp_del, pickup = extract(segment)
        else:
            status, dest, exp_del, pickup = "NOT_FOUND", "N/A", "N/A", "N/A"
        rows.append([str(wid), status, dest, exp_del, pickup, current_time])

    return rows


def scrape_batch(driver, waybill_ids, current_time):
    wait = WebDriverWait(driver, PAGE_WAIT)
    error_row = lambda reason: [[str(w), reason, "N/A", "N/A", "N/A", current_time] for w in waybill_ids]

    try:
        driver.get(TRACKING_URL)
        time.sleep(3)
    except Exception as e:
        print(f"  ERROR loading page: {e}")
        return error_row("PAGE_LOAD_ERROR")

    try:
        textarea = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "textarea.form-control")))
        textarea.clear()
        time.sleep(0.3)
        textarea.send_keys(", ".join(str(w) for w in waybill_ids))
        time.sleep(0.3)
    except Exception as e:
        print(f"  ERROR filling textarea: {e}")
        return error_row("INPUT_ERROR")

    try:
        btn = next(
            (b for b in driver.find_elements(By.CSS_SELECTOR, "button")
             if b.text.strip().lower() == "track" and b.is_displayed()),
            None
        ) or wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button[class*='track'], button.btn-danger")))
        driver.execute_script("arguments[0].click();", btn)
    except Exception as e:
        print(f"  ERROR clicking Track: {e}")
        return error_row("BTN_ERROR")

    wait_for_results(driver)
    return parse_results(driver, waybill_ids, current_time)


def main():
    print("Connecting to Google Sheets...")
    try:
        target_doc = get_google_targetdoc()
        doc          = get_google_doc()
        input_sheet  = doc.worksheet("NAQEL_TRACK_ID")
        output_sheet = prepare_output_sheet(target_doc)
    except Exception as e:
        print(f"Google Sheets error: {e}")
        sys.exit(1)

    # naqel_ids = [ row[0] for row in input_sheet.get("A2:F") if len(row) >= 6 and str(row[5]).strip().upper() == "NAQEL" and row[0]]
    naqel_ids = [ row[0] for row in input_sheet.get("A2:F")]

    if not naqel_ids:
        print("No NAQEL records found in TRACK_DATA (Column F must say 'NAQEL').")
        return

    total_batches = (len(naqel_ids) + BATCH_SIZE - 1) // BATCH_SIZE
    print(f"Found {len(naqel_ids)} Naqel IDs — {total_batches} batch(es) of up to {BATCH_SIZE}.")
    current_time = time.strftime("%Y-%m-%d %H:%M:%S")

    print("Starting browser...")
    try:
        driver = create_driver()
    except Exception as e:
        print(f"Failed to launch Chrome: {e}")
        sys.exit(1)

    all_rows = []
    try:
        for batch_num, i in enumerate(range(0, len(naqel_ids), BATCH_SIZE), start=1):
            batch = naqel_ids[i:i + BATCH_SIZE]
            print(f"\nBatch {batch_num}/{total_batches} ({len(batch)} IDs)...")
            rows = scrape_batch(driver, batch, current_time)
            all_rows.extend(rows)
            if rows:
                output_sheet.update(values=rows, range_name=f"A{i + 2}")
                print(f"  Written {len(rows)} rows.")
            if batch_num < total_batches:
                time.sleep(3)
    except KeyboardInterrupt:
        print("\n[STOPPED]")
    finally:
        driver.quit()

    print(f"\nDone. Total rows written: {len(all_rows)}")



if __name__ == "__main__":
    print("Naqel status fetching.")
    try:
        while True:
            main()
            print(f"Sleeping for 30 minutes...")
            # time.sleep(60)
            time.sleep(3600)
    except KeyboardInterrupt:
        print("\nShutdown requested.")
        sys.exit(0)
