import os
import sys
import time
import requests
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ─────────────────────────────────────────────
#  CONFIGURATION  — edit these values
# ─────────────────────────────────────────────


SHEET_ID = "1hClSA4u_gE5KUudt2Vz2dHJVmh9a3AciCjycjB3Rvds"
TARGET_SHEET = "1S6WaI1rXuf5ogN44ph8weA_z_bMCg85U3uaZ7RHvpfM"
# LOGISTIC_SHEET_ID = "1S6WaI1rXuf5ogN44ph8weA_z_bMCg85U3uaZ7RHvpfM"

INPUT_RANGE = "RGS_TRACK_ID!A2:H"  
OUTPUT_RANGE = "RGS_STATUS!A2:Z"

COD_EMAIL = "info@emirath.com"
COD_PASSWORD = "1784497"

SERVICE_ACCOUNT_FILE = r"C:\Users\USER\Downloads\Emarath_global\service_account.json"


BATCH_SIZE           = 50    # how many AWBs per API call
BATCH_DELAY_S        = 2     # seconds to wait between batches
WRITE_SHEET_DELAY_S  = 3     # seconds to wait between writing to each sheet
SYNC_INTERVAL_S      = 1800  # 1 hour between full sync cycles

LOCK_FILE            = os.path.join(os.path.dirname(os.path.abspath(__file__)), "rgs_tracker.lock")
# ─────────────────────────────────────────────


# ── Date formatting helper ────────────────────────────────────────────────────

def format_date(value: str) -> str:
    """
    Attempt to parse a date string in various common formats and
    return it as MM/DD/YYYY.  Returns the original string unchanged
    if it cannot be parsed.
    """
    if not value or not value.strip():
        return value

    # Formats to try, in order of likelihood
    formats = [
        "%Y-%m-%d",          # 2025-07-21
        "%d/%m/%Y",          # 21/07/2025
        "%m/%d/%Y",          # 07/21/2025  (already target — normalise anyway)
        "%d-%m-%Y",          # 21-07-2025
        "%Y/%m/%d",          # 2025/07/21
        "%d %b %Y",          # 21 Jul 2025
        "%d %B %Y",          # 21 July 2025
        "%b %d, %Y",         # Jul 21, 2025
        "%B %d, %Y",         # July 21, 2025
        "%Y-%m-%dT%H:%M:%S", # ISO 8601 with time
        "%Y-%m-%d %H:%M:%S", # datetime with space separator
    ]

    raw = value.strip().lstrip("'")  # remove leading apostrophe (Sheets text-force prefix)
    for fmt in formats:
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.strftime("%m/%d/%Y")
        except ValueError:
            continue

    # Could not parse — return as-is so no data is silently lost
    return value


# ── Cross-platform single-instance lock ───────────────────────────────────────

class SingleInstanceLock:

    def __init__(self, path):
        self.path = path
        self.acquired = False

    def acquire(self):
        if os.path.exists(self.path):
            try:
                with open(self.path, "r") as f:
                    old_pid = int(f.read().strip())
                if self._pid_alive(old_pid):
                    print(
                        f"[ERROR] Another instance is already running (PID {old_pid}). Exiting."
                    )
                    return False
                else:
                    print(f"[INFO] Stale lock file found (PID {old_pid} is gone). Removing it.")
                    os.remove(self.path)
            except (ValueError, OSError):
                try:
                    os.remove(self.path)
                except OSError:
                    pass

        with open(self.path, "w") as f:
            f.write(str(os.getpid()))
        self.acquired = True
        return True

    def release(self):
        if self.acquired and os.path.exists(self.path):
            try:
                os.remove(self.path)
            except OSError:
                pass
        self.acquired = False

    @staticmethod
    def _pid_alive(pid):
        """Returns True if a process with the given PID is running."""
        try:
            os.kill(pid, 0)
            return True
        except (OSError, ProcessLookupError):
            return False


# ── Google Sheets client ──────────────────────────────────────────────────────

def get_sheets_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=scopes
    )
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


# ── COD login ─────────────────────────────────────────────────────────────────

def get_token():
    url    = "https://codsolution.co/ship/Api/loginApi"
    params = {"email": COD_EMAIL, "password": COD_PASSWORD}
    try:
        response = requests.post(url, params=params, timeout=15)
        data     = response.json()
        token    = data.get("bearer_token")
        if not token:
            print("COD login failed: No token in response. Check credentials.")
            return None
        return token
    except Exception as e:
        print(f"Login Error: {e}")
        return None


# ── Read AWBs from Google Sheets (with exponential back-off) ──────────────────

def read_awbs(sheets_service, retries=6):
    """
    Read AWB rows from the input sheet.
    Retries up to `retries` times on HTTP 429 (rate-limit) errors,
    using exponential back-off (3s, 5s, 9s, 17s, 33s, 65s).
    """
    sheet = sheets_service.spreadsheets()

    for attempt in range(retries):
        try:
            result = sheet.values().get(
                spreadsheetId=SHEET_ID,
                range=INPUT_RANGE
            ).execute()

            values = result.get("values", [])
            if not values:
                print("No AWB data found in sheet.")
                return []

            awb_rows = []
            for row in values:
                if len(row) > 0 and str(row[0]).strip():
                    awb_rows.append({
                        "awb":         str(row[0]).strip(),
                        "DATE":        format_date(str(row[1]).strip()) if len(row) > 1 else "",
                        "description": row[2]              if len(row) > 2 else "",
                        "NAME":        row[3]              if len(row) > 3 else "",
                        "NUMBER1":     row[4]              if len(row) > 4 else "",
                    })
            return awb_rows

        except HttpError as e:
            if e.resp.status == 429:
                wait = (2 ** attempt) + 1   # 3, 5, 9, 17, 33, 65 seconds
                print(
                    f"[Rate Limit] Google Sheets read quota hit. "
                    f"Retrying in {wait}s... (attempt {attempt + 1}/{retries})"
                )
                time.sleep(wait)
            else:
                print(f"Read Error (HTTP {e.resp.status}): {e}")
                return []
        except Exception as e:
            print(f"Read Error: {e}")
            return []

    print("Max retries exceeded for read_awbs. Skipping this cycle.")
    return []


# ── Helpers ───────────────────────────────────────────────────────────────────

def chunk_list(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i: i + n]


# ── Bulk tracking via COD API ─────────────────────────────────────────────────

def track_bulk(token, awb_rows):
    batches     = list(chunk_list(awb_rows, BATCH_SIZE))
    all_results = []
    url         = "https://codsolution.co/ship/Api/track_parcel_bulk"
    headers     = {"Authorization": f"Bearer {token}"}

    for i, batch in enumerate(batches):
        print(f"Tracking batch {i + 1}/{len(batches)}...")
        try:
            tracking_numbers = [item["awb"] for item in batch]
            response = requests.post(
                url,
                json={"tracking_numbers": tracking_numbers},
                headers=headers,
                timeout=20
            )

            # Stop immediately on COD rate-limit
            if response.status_code == 429 or "Too many requests" in response.text:
                print(
                    "!!! COD RATE LIMIT TRIGGERED !!! "
                    "Stopping this cycle to avoid a permanent block."
                )
                break

            try:
                data_obj = response.json()
            except ValueError:
                print(f"API Error: Non-JSON response in batch {i + 1}")
                continue

            if not isinstance(data_obj, dict):
                print(f"Batch {i + 1} failed: API returned {type(data_obj)}")
                continue

            for awb, api_data in data_obj.items():
                meta    = next((item for item in batch if item["awb"] == awb), None)
                is_dict = isinstance(api_data, dict)

                all_results.append({
                    "DATE":      meta["DATE"]    if meta else "",
                    "awb":       awb,
                    "reference": api_data.get("ShipmentReference", "") if is_dict else "",
                    "NAME":      meta["NAME"]    if meta else "",
                    "NUMBER1":   meta["NUMBER1"] if meta else "",
                    "country":   "KSA",
                    "status":    api_data.get("Status",  "N/A") if is_dict else "ERROR",
                    "date_api":  format_date(api_data.get("date", "")) if is_dict else "",
                    "time":      api_data.get("time",    "")    if is_dict else "",
                    "comment":   api_data.get("comment", "")    if is_dict else str(api_data),
                })

        except Exception as e:
            print(f"Batch {i + 1} Processing Error: {e}")

        time.sleep(BATCH_DELAY_S)

    return all_results


# ── Write results to Google Sheets (with exponential back-off) ────────────────

def write_status(sheets_service, data):
    if not data:
        print("No data to write.")
        return

    rows = [
        [
            d["DATE"],     d["awb"],     d["reference"], d["NAME"],
            d["NUMBER1"],  d["country"], d["status"],
            d["date_api"], d["time"],    d["comment"]
        ]
        for d in data
    ]

    target_sheets = [TARGET_SHEET]
    sheet         = sheets_service.spreadsheets()

    for sid in target_sheets:
        for attempt in range(6):
            try:
                # 1. Clear existing data
                sheet.values().clear(
                    spreadsheetId=sid,
                    range=OUTPUT_RANGE
                ).execute()

                time.sleep(1)   # small pause between clear and update

                # 2. Write new data
                sheet.values().update(
                    spreadsheetId=sid,
                    range=OUTPUT_RANGE,
                    valueInputOption="USER_ENTERED",
                    body={"values": rows}
                ).execute()

                print(f"Successfully updated Spreadsheet ID: {sid} ({len(rows)} rows)")
                break   # success — move to next sheet

            except HttpError as e:
                if e.resp.status == 429:
                    wait = (2 ** attempt) + 1
                    print(
                        f"[Rate Limit] Google Sheets write quota hit for {sid}. "
                        f"Retrying in {wait}s... (attempt {attempt + 1}/6)"
                    )
                    time.sleep(wait)
                else:
                    print(f"Write Error for Sheet {sid} (HTTP {e.resp.status}): {e}")
                    break
            except Exception as e:
                print(f"Write Error for Sheet {sid}: {e}")
                break

        # Pause between writing to each sheet to avoid quota spikes
        time.sleep(WRITE_SHEET_DELAY_S)


# ── Main cycle ────────────────────────────────────────────────────────────────

def run_tracker(service):
    print("\n--- Starting Update Cycle ---")

    awb_rows = read_awbs(service)
    print(f"Found {len(awb_rows)} AWBs.")

    if not awb_rows:
        print("Nothing to track. Ending cycle.")
        return

    token = get_token()
    if not token:
        print("Could not obtain COD token. Ending cycle.")
        return

    results = track_bulk(token, awb_rows)
    if results:
        write_status(service, results)
    else:
        print("No tracking results returned.")

    print("--- Cycle Finished ---")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    lock = SingleInstanceLock(LOCK_FILE)

    if not lock.acquire():
        sys.exit(1)

    print("RGS Tracker Service is running. Press Ctrl+C to stop.")
    service = get_sheets_client()

    try:
        while True:
            try:
                run_tracker(service)
            except Exception as e:
                print(f"Unexpected System Error: {e}")

            print(f"Waiting {SYNC_INTERVAL_S // 60} minutes for next sync...")
            time.sleep(SYNC_INTERVAL_S)

    except KeyboardInterrupt:
        print("\n[STOPPED] Shutdown signal received.")
        print("Exiting RGS AWB Tracker gracefully... Goodbye!")
        lock.release()
        sys.exit(0)