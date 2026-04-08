import pandas as pd
import numpy as np
import time
import warnings
import sys
from datetime import datetime
from gspread_pandas import Spread, conf

# Ignore deprecation warnings
warnings.filterwarnings('ignore', category=DeprecationWarning)

# --- CONFIGURATION ---
CONFIG_PATH = r'C:\Users\USER\Downloads\Emarath_global\service_account.json'

LOGISTIC_URLS = [
    # "https://docs.google.com/spreadsheets/d/1ZccUbe1L-suAl-0aEW6p6lj2j263VwEmk0HRkvjCKKs/edit?",
    "https://docs.google.com/spreadsheets/d/1FTPSWfc0hNab2-cYqypfF4pyOvwHBkbHVYcOsJwaH24/edit?gid=0#gid=0",
    "https://docs.google.com/spreadsheets/d/18hPOfPcilqGoyIPiXikP5Geb3D0NbDJpTDHQCTAc_Lc/edit?gid=0#gid=0",
]

SALES_URLS = [
    # "https://docs.google.com/spreadsheets/d/1aXobnCl4QQ-hQ_IWoTPo2ZEo3HM5kNwycwMBJtEDOfc/edit",
    "https://docs.google.com/spreadsheets/d/1ztfkIIEeL9EmSdLQIGhBGIFbraMLmqEBwc8FP-CA_3c/edit",
    "https://docs.google.com/spreadsheets/d/1PagWGqwt9WhpaCSvopMydXaPlsLSlZ2FrAdQMfMSK1g/edit",
]

TARGET_URL = ["https://docs.google.com/spreadsheets/d/19B1Vclt9U0AOu-yk99E960g5tYRQ9JRX-M2GlAPvE6U/edit?"]

TEAM_HISTORY = [
    ('RAHIB', None, 'SHAHABAS TEAM'), ('HABIYA', None, 'SHAHABAS TEAM'),
    ('SHAMNA', None, 'SHAHABAS TEAM'), ('ARUN', None, 'SHAHABAS TEAM'),
    ('CHAITHANYA', None, 'SHAHABAS TEAM'), ('BURHANA', None, 'SHAHABAS TEAM'), 
    ('ARSHAD', None, 'MANASA TEAM'), ('NAJIYA', None, 'MANASA TEAM'), ('SHIHAD', None, 'MANASA TEAM'), ('ADWAITHA', None, 'MANASA TEAM'),
    ('ZAKIYA', None, 'MANASA TEAM'), ('SAFAN', None, 'MANASA TEAM'), ('NIHAD', None, 'MANASA TEAM'),
    ('CHAITHANYA', '2026-03-31', 'SUPER SALES TEAM'), ('HABIYA', '2026-03-31', 'SUPER SALES TEAM'), ('SAFAN', '2026-03-31', 'SUPER SALES TEAM'), 
    ('NIHAD', '2026-03-31', 'SUPER SALES TEAM'), ('AMINA', '2026-03-31', 'SUPER SALES TEAM'), ('RAHIYAD', '2026-03-31', 'SUPER SALES TEAM'),
    ('NAFI', None, 'JEFFIN TEAM'), ('AKASH', None, 'JEFFIN TEAM'), ('GOWTHAM', None, 'JEFFIN TEAM'), ('AMINA', None, 'JEFFIN TEAM'), ('RINSIYA', None, 'JEFFIN TEAM'), 
    ('NEHA', None, 'JEFFIN TEAM'), ('RANJITH', None, 'JEFFIN TEAM'), ('AFNAN', None, 'JEFFIN TEAM'), ('RAHIYAD', None, 'JEFFIN TEAM'),
    ('ADWAITHA', '2026-03-17', 'SHAHABAS TEAM'),
    ('AKASH', '2026-04-03', 'MANASA TEAM'), ('NEHA', '2026-04-03', 'MANASA TEAM'), ('AFNAN', '2026-04-03', 'SHAHABAS TEAM'), ('RANJITH', '2026-04-03', 'SHAHABAS TEAM'),
    ('AFNAN', '2026-04-06', 'SHAHABAS TEAM'), ('RANJITH', '2026-04-06', 'SHAHABAS TEAM'), ('ADWAITHA', '2026-04-06', 'SHAHABAS TEAM'),
    
    
]

BDE_NAMES = list(set([h[0] for h in TEAM_HISTORY]))

def clean_duplicate_columns(df):
    cols = pd.Series(df.columns)
    for dup in cols[cols.duplicated()].unique(): 
        cols[cols == dup] = [f"{dup}.{i}" if i != 0 else dup for i in range(sum(cols == dup))]
    df.columns = cols
    return df

def get_team_for_date(agent, record_date):
    if pd.isna(record_date) or not agent:
        for name, _, team in TEAM_HISTORY:
            if name == agent: return team
        return ''
    
    try:
        record_dt = pd.to_datetime(record_date)
        agent_history = [h for h in TEAM_HISTORY if h[0] == str(agent).strip().upper()]
        agent_history.sort(key=lambda x: pd.to_datetime(x[1] or '1900-01-01'), reverse=True)
        
        for _, start_str, team in agent_history:
            start_dt = pd.to_datetime(start_str or '1900-01-01')
            if record_dt >= start_dt:
                return team
    except: pass
    return ''

def format_date_to_string(df, col_name='DATE'):
    """Forces date column to YYYY-MM-DD string format."""
    if col_name in df.columns:
        # Convert to datetime objects first to handle multiple input formats
        df[col_name] = pd.to_datetime(df[col_name], errors='coerce', dayfirst=True)
        # Convert back to standardized string YYYY-MM-DD
        df[col_name] = df[col_name].dt.strftime('%Y-%m-%d').replace('NaT', '')
    return df

def safe_upload(spreadsheet, df, sheet_name):
    if not df.empty:
        try:
            spreadsheet.open_sheet(sheet_name, create=True)
            curr_rows = spreadsheet.sheet.row_count
            needed_rows = len(df) + 10
            
            if curr_rows < needed_rows:
                spreadsheet.sheet.resize(rows=needed_rows)
            
            spreadsheet.sheet.batch_clear([f'A2:ZZ{max(curr_rows, 1000)}'])
            
            # Final replacement of any lingering null types before upload
            upload_df = df.astype(str).replace(['nan', 'None', 'NaT', '<NA>'], '')
            spreadsheet.df_to_sheet(upload_df, index=False, replace=False, headers=False, start="A2")
            print(f"[✓] Uploaded {len(df)} rows to {sheet_name}")
        except Exception as e:
            print(f"[!] Upload failed for {sheet_name}: {e}")

def run_update():
    print(f"\n--- Sync started at {datetime.now().strftime('%H:%M:%S')} ---")
    try:
        c = conf.get_config(file_name=CONFIG_PATH)
        target_spread = Spread(TARGET_URL[0], config=c)
        
        # # --- 1. LOGISTIC DATA PROCESSING ---
        # all_logistic_dfs = []
        # for url in LOGISTIC_URLS:
        #     try:
        #         spread = Spread(url, config=c)
        #         raw_data = spread.sheet_to_df(sheet="CRM", index=None)
        #         if not raw_data.empty:
        #             raw_data = clean_duplicate_columns(raw_data)
        #             raw_data = format_date_to_string(raw_data, 'DATE')
        #             all_logistic_dfs.append(raw_data)
        #     except Exception as e: print(f" ! Error Logistic URL {url}: {e}")

        all_logistic_dfs = []
        for url in LOGISTIC_URLS:
            try:
                spread = Spread(url, config=c)
                raw_data = spread.sheet_to_df(sheet="CRM", index=None)
                if not raw_data.empty:
                    raw_data = clean_duplicate_columns(raw_data)
                    raw_data = format_date_to_string(raw_data, 'DATE')
                    
                    # Filter: Keep only EMARATH GLOBAL rows
                    if 'Customer Path' in raw_data.columns:
                        raw_data = raw_data[raw_data['Customer Path'].str.strip().str.upper() == 'EMARATH GLOBAL']
                    
                    if not raw_data.empty:
                        all_logistic_dfs.append(raw_data)
            except Exception as e:
                print(f" ! Error Logistic URL {url}: {e}")

        if all_logistic_dfs:
            logistic_df = pd.concat(all_logistic_dfs, ignore_index=True, sort=False)
            safe_upload(target_spread, logistic_df, "LOGISTC_DATA")

        # --- 2. SALES DATA PROCESSING ---
        all_sales_list = []
        invalid_vals = ['', 'nan', 'none', 'n/a', '0', '0.0', 'nat', '<na>']

        for url in SALES_URLS:
            try:
                source_spread = Spread(url, config=c)
                existing_tabs = [s.title for s in source_spread.sheets]
                for bde in BDE_NAMES:
                    if bde not in existing_tabs: continue
                    
                    df = source_spread.sheet_to_df(sheet=bde, index=None)
                    if df.empty: continue
                    
                    df = clean_duplicate_columns(df)
                    
                    # Unified Cleaning: AGENT, DATE, PHONE NO 1
                    cols_to_check = [c for c in ['AGENT', 'DATE', 'PHONE NO 1'] if c in df.columns]
                    for col in cols_to_check:
                        df[col] = df[col].astype(str).str.strip().replace('nan', '')
                        df = df[~df[col].str.lower().isin(invalid_vals)]

                    # Standardize Date to YYYY-MM-DD
                    df = format_date_to_string(df, 'DATE')
                    
                    # Remove any rows where DATE became empty after formatting
                    df = df[df['DATE'] != '']
                    
                    if 'AGENT' in df.columns:
                        df['BDM_TEAM'] = df.apply(lambda x: get_team_for_date(x.get('AGENT'), x.get('DATE')), axis=1)
                    
                    all_sales_list.append(df)
            except Exception as e: print(f"Failed Sales URL {url}: {e}")

        if all_sales_list:
            master_sales_df = pd.concat(all_sales_list, ignore_index=True, sort=False)
            safe_upload(target_spread, master_sales_df, "SALES_DATA")

    except Exception as e: print(f"CRITICAL ERROR: {e}")

if __name__ == "__main__":
    print("--- BDM Dashboard Automation Started ---")
    try:
        while True:
            run_update()
            print("Waiting for 15 minutes...")
            time.sleep(900)
    except KeyboardInterrupt:
        print("Automation stopped. Goodbye!")
        sys.exit(0)