import pandas as pd
import numpy as np
import time
import sys
import warnings
import traceback
from datetime import datetime
from gspread_pandas import Spread, conf

# Suppress deprecation warnings
warnings.filterwarnings('ignore', category=DeprecationWarning)

# --- CONFIGURATION ---
CONFIG_PATH = r'C:\Users\USER\Downloads\Emarath_global\service_account.json'
TARGET_URL = "https://docs.google.com/spreadsheets/d/1kQYJ3qi_BWDGthcw2ZplfaGkM7dxqi-Q9ArN63S9F_Q/edit?gid=0#gid=0"

# --- SCALABLE SOURCE URLS ---
SOURCE_URLS = [
    "https://docs.google.com/spreadsheets/d/1aXobnCl4QQ-hQ_IWoTPo2ZEo3HM5kNwycwMBJtEDOfc/edit",
    "https://docs.google.com/spreadsheets/d/1ztfkIIEeL9EmSdLQIGhBGIFbraMLmqEBwc8FP-CA_3c/edit"
]

# --- COMMON BDE LIST ---
BDE_NAMES = [
    'RAHIB', 'SHAMNA', 'HABIYA', 'BURHANA'
    'CHAITHANYA', 'ZAKIYA', 'SAFAN', 'NAJIYA',
    'ADWAITHA', 'NEHA', 'GOWTHAM', 'AMINA', 'NAFI',
    'RINSIYA', 'ARSHAD', "SHIHAD", "RAHIYAD", "AKASH", 'RANJITH',
    "SHYAMJIL", 'ADHIL', "HIBA", 'NAJA', 'AJIN', 'SHABNA', 'FARSANA', 'SUSHANTHIKA',
    'ARUN', "NIHAD"
]

LEAD_STATUSES = ["WON", "SUPER HOT", "HOT", "WARM", "COLD", "BOOKING", "WHATS APP ENGAGE"]
SYNC_INTERVAL_SECONDS = 3600

def deduplicate_columns(df):
    if df.columns.duplicated().any():
        cols = pd.Series(df.columns)
        seen = {}
        new_cols = []
        for col in cols:
            if col in seen:
                seen[col] += 1
                new_cols.append(f"{col}.{seen[col]}")
            else:
                seen[col] = 0
                new_cols.append(col)
        df.columns = new_cols
    return df

def safe_upload(spreadsheet, df, sheet_name):
    """Clears and uploads the FULL combined data from all sources."""
    if not df.empty:
        try:
            spreadsheet.open_sheet(sheet_name, create=True)
            
            # Grid Resize logic
            needed_rows = len(df) + 10
            needed_cols = len(df.columns) + 1
            curr_rows = spreadsheet.sheet.row_count
            
            if curr_rows < needed_rows:
                spreadsheet.sheet.resize(rows=needed_rows)

            # Clear existing data (from A2 downwards) before writing the NEW combined list
            spreadsheet.sheet.batch_clear([f'A2:Z{max(curr_rows, 5000)}'])
            
            upload_df = df.astype(str).replace(['nan', 'None', 'NaT'], '')
            spreadsheet.df_to_sheet(upload_df, index=False, replace=False, headers=False, start="A2")
        except Exception as e:
            print(f"  [!] Upload failed for {sheet_name}: {e}")

def run_sync_process():
    print(f"\n--- Sync started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")

    try:
        c = conf.get_config(file_name=CONFIG_PATH)
        target_spread = Spread(TARGET_URL, config=c)
        
        # This list will hold data from URL 1, URL 2, etc.
        all_data_list = []

        mapping_cols = {
            'REF NO': 'REF NO', 'COUNTRY': 'REGION', 'DATE': 'DATE',
            'AGENT': 'AGENT', 'CUSTOMER PATH': 'CUSTOMER PATH', 'NAME': 'NAME',
            'PHONE NO 1': 'PHONE NO', 'STATUS': 'STATUS', 'PRODUCT 1': 'PRODUCT',
            'CALL STATUS': 'CALL STATUS'
        }

        # 1. LOOP THROUGH ALL SOURCE URLS
        for url in SOURCE_URLS:
            print(f" [>] Accessing Source: {url[-15:]}...")
            try:
                source_spread = Spread(url, config=c)
                existing_tabs = [s.title for s in source_spread.sheets]

                # 2. LOOP THROUGH ALL BDE NAMES WITHIN THIS URL
                for bde in BDE_NAMES:
                    if bde not in existing_tabs:
                        continue

                    try:
                        df = source_spread.sheet_to_df(sheet=bde, index=None, header_rows=1)
                        df = deduplicate_columns(df)

                        if not df.empty and all(col in df.columns for col in mapping_cols.keys()):
                            subset = df[list(mapping_cols.keys())].copy()
                            subset.rename(columns=mapping_cols, inplace=True)
                            
                            # Add a source tag (Optional, helps identify where data came from)
                            subset['DATA_SOURCE'] = url[-15:] 
                            
                            subset['DATE'] = pd.to_datetime(subset['DATE'], errors='coerce', dayfirst=True)

                            # Remove empty phone numbers
                            subset = subset[~subset['PHONE NO'].astype(str).str.strip().str.upper().isin(['', 'NAN', '0'])]

                            if not subset.empty:
                                for col in ['PHONE NO', 'CUSTOMER PATH', 'CALL STATUS', 'STATUS']:
                                    subset[col] = subset[col].astype(str).str.strip().str.upper()
                                
                                # APPEND this subset to our master collection list
                                all_data_list.append(subset)
                                
                    except Exception as e:
                        print(f"    [!] Error in {bde} on sheet {url[-10:]}: {e}")
            except Exception as e:
                print(f"  [!] Failed to reach URL {url}: {e}")

        if not all_data_list:
            print("  [-] No data found in any sources.")
            return

        # --- 3. THE "APPEND" STEP ---
        # This merges all data from URL 1 and URL 2 into one single master table
        master_df = pd.concat(all_data_list, ignore_index=True)
        print(f" [OK] Combined {len(master_df)} total rows from all sources.")

        # --- 4. SPLIT BY STATUS AND UPLOAD ---
        # Unattended
        unattended_mask = master_df['CALL STATUS'].isin(['', 'NAN', 'NONE', 'N/A', 'UNMARKED'])
        unattended_df = master_df[unattended_mask].copy()
        if not unattended_df.empty:
            unattended_df = unattended_df.sort_values(by=['AGENT', 'DATE'])
            safe_upload(target_spread, unattended_df, "UNATTENDED_LEADS")

        # Attended Statuses
        attended_df = master_df[~unattended_mask].copy()
        for status in LEAD_STATUSES:
            status_df = attended_df[attended_df['STATUS'] == status.upper()].copy()
            if not status_df.empty:
                status_df = status_df.sort_values(by=['DATE'])
                tab_name = status.replace(' ', '_')[:31]
                safe_upload(target_spread, status_df, tab_name)

        print(f"--- Sync Success at {datetime.now().strftime('%H:%M:%S')} ---")

    except Exception as e:
        print(f"  [CRITICAL ERROR]: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    try:
        while True:
            run_sync_process()
            time.sleep(SYNC_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\nShutdown complete.")