import pandas as pd
import numpy as np
import time
import sys
import warnings
import traceback
from datetime import datetime
from gspread_pandas import Spread, conf

# Suppress warnings
warnings.filterwarnings('ignore', category=DeprecationWarning)

# --- CONFIGURATION ---
CONFIG_PATH = r'C:\Users\USER\Downloads\Emarath_global\service_account.json'
LOGISTIC_SOURCES = [
    "https://docs.google.com/spreadsheets/d/1ZccUbe1L-suAl-0aEW6p6lj2j263VwEmk0HRkvjCKKs/edit?userstoinvite=shamseena.emarath%40gmail.com&sharingaction=manageaccess&role=writer&gid=0#gid=0",
    "https://docs.google.com/spreadsheets/d/1pVBNDiZcjlZnW5-JdsrXsovBr4j7mf9gvrKLlZpfkiU/edit?gid=0#gid=0",
]
TARGET_SPREAD_URL = "https://docs.google.com/spreadsheets/d/1hClSA4u_gE5KUudt2Vz2dHJVmh9a3AciCjycjB3Rvds/edit?pli=1&gid=382052561#gid=382052561"
MULTI_TARGET_SPREAD_URLS = [
    "https://docs.google.com/spreadsheets/d/1hClSA4u_gE5KUudt2Vz2dHJVmh9a3AciCjycjB3Rvds/edit?pli=1&gid=382052561#gid=382052561",
    "https://docs.google.com/spreadsheets/d/1OU786oBbozzYkvr2O9WUG3IECJfLgxVwDOt_lZRsAKo/edit?gid=0#gid=0"
]

# Fixed: Added comma after 'PRODUCT 1'
REQUIRED_COLUMNS = [
    'COUNTRY', 'AGENT', 'DATE', 'TRACKING NUMBER', 'EM NUMBER', 'NAME', 'NUMBER1', 'PRODUCT 1',
    'VALUE', 'PAYMENT METHOD', 'DELIVERY AGENTS', 'STATUS', 'DISPATCHED DATE', 'PRODUCT 2', 'QTY 2'
]

COLUMN_RENAME_MAP = {
    'TRACKING NUM': 'TRACKING NUMBER',
    'EMNUMBER': 'EM NUMBER',
    r'DISPATCHED\nDATE': 'DISPATCHED DATE',
}

def standardize_df(df, fallback_country="UNKNOWN"):
    if df.empty:
        return pd.DataFrame(columns=REQUIRED_COLUMNS)
    
    df.columns = df.columns.astype(str).str.strip().str.upper()
    df.columns = [c.replace('\n', ' ') for c in df.columns]
    
    rename_upper = {k.upper().replace(r'\N', ' '): v.upper() for k, v in COLUMN_RENAME_MAP.items()}
    df = df.rename(columns=rename_upper)
    
    if 'COUNTRY' in df.columns:
        df['COUNTRY'] = df['COUNTRY'].replace('', np.nan).ffill().bfill()
    else:
        df['COUNTRY'] = fallback_country
    
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            df[col] = np.nan

    df = df[REQUIRED_COLUMNS].copy()
    # Ensure AGENT and EM NUMBER exist before masking
    agent_mask = df['AGENT'].astype(str).str.strip().ne('') if 'AGENT' in df.columns else False
    em_mask = df['EM NUMBER'].astype(str).str.strip().ne('') if 'EM NUMBER' in df.columns else False
    return df[agent_mask | em_mask]

def safe_upload(spreadsheet, sheet_name, df):
    if df.empty:
        print(f"   - Skipping {sheet_name}: No data to upload.")
        return

    spreadsheet.open_sheet(sheet_name, create=True)
    last_row = spreadsheet.sheet.row_count
    if last_row > 1:
        spreadsheet.sheet.batch_clear([f'A2:Z{last_row}'])
    
    needed_rows = len(df) + 10
    if spreadsheet.sheet.row_count < needed_rows:
        spreadsheet.sheet.resize(rows=needed_rows)

    upload_df = df.fillna("").astype(str)
    spreadsheet.df_to_sheet(upload_df, index=False, replace=False, headers=False, start="A2")

def run_report_cycle():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Starting update cycle...")
    try:
        c = conf.get_config(file_name=CONFIG_PATH)
        target_spread = Spread(TARGET_SPREAD_URL, config=c)
        all_dfs = []

        # 1. Load Monthly Sources
        for url in LOGISTIC_SOURCES:
            try:
                spread = Spread(url, config=c)
                for sheetname in ["CRM"]:
                    try:
                        raw_data = spread.sheet_to_df(sheet=sheetname, index=None)
                        if not raw_data.empty:
                            # FIX: Changed 'fallback_sheet' to 'fallback_country' to match function definition
                            standardized = standardize_df(raw_data, fallback_country="KSA")
                            all_dfs.append(standardized)
                    except Exception as e:
                        print(f"   ! Error in sheet {sheetname} at {url}: {e}")
            except Exception as e:
                print(f"   ! Could not access URL {url}: {e}")

        if not all_dfs:
            print("No data found to update.")
            return

        # 2. Combine and Clean
        df = pd.concat(all_dfs, ignore_index=True)
        
        for col in ['DATE', 'DISPATCHED DATE']:
            df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')

        df['TRACKING NUMBER'] = df['TRACKING NUMBER'].replace(r'^\s*$', np.nan, regex=True)
        df_clean = df.dropna(subset=['TRACKING NUMBER']).copy()
        
        # 3. Main Upload
        df_display = df_clean.copy()
        # for col in ['DATE', 'DISPATCHED DATE']:
        #     df_display[col] = df_display[col].dt.strftime('%d/%m/%Y').fillna("")
        
        # safe_upload(target_spread, 'DISPATCH', df_display)
        print(f"Pushing DISPATCH data to {len(MULTI_TARGET_SPREAD_URLS)} targets...")
        for target_url in MULTI_TARGET_SPREAD_URLS:
            try:
                t_spread = Spread(target_url, config=c)
                safe_upload(t_spread, 'DISPATCH', df_display)
                print(f"   - Successfully updated DISPATCH in: {target_url[:45]}...")
            except Exception as e:
                print(f"   ! Failed to upload to {target_url}: {e}")

        # 4. Pending & Aging Logic
        today = pd.Timestamp.now().normalize()
        # exclude = ["DELIVERED", "DELIVERED AND UNPAID", "RTO", "CANCELLED"]
        exclude = ["SALES CLOSED", "DELIVERED/UNPAID", "RTO-ASSIGNED", "CANCELLD&RETURN"]
        
        pending_df = df_clean[~df_clean['STATUS'].astype(str).str.upper().isin(exclude)].copy()
        
        # Calculate Age
        pending_df['AGE_DAYS'] = (today - pending_df['DISPATCHED DATE']).dt.days

        segments = {
            'PENDING': pending_df,
            '1-2 DAYS': pending_df[pending_df['AGE_DAYS'].between(1, 2)],
            '3-4 DAYS': pending_df[pending_df['AGE_DAYS'].between(3, 4)],
            '5-6 DAYS': pending_df[pending_df['AGE_DAYS'].between(5, 6)],
            '7 Days':   pending_df[pending_df['AGE_DAYS'] == 7],
            'Alert':    pending_df[pending_df['AGE_DAYS'] > 7]
        }

        for name, seg_df in segments.items():
            seg_disp = seg_df.drop(columns=['AGE_DAYS'], errors='ignore').copy()
            for col in ['DATE', 'DISPATCHED DATE']:
                seg_disp[col] = seg_disp[col].dt.strftime('%m/%d/%Y').fillna("")
            safe_upload(target_spread, name, seg_disp)

        print(f"[{datetime.now().strftime('%H:%M:%S')}] Cycle completed: {len(df_clean)} rows processed.")

    except Exception as e:
        print(f"   ! CRITICAL ERROR: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    try:
        while True:
            run_report_cycle()
            print("Sleeping for 15 minutes...")
            time.sleep(900)
    except KeyboardInterrupt:
        print("\nShutdown complete.")