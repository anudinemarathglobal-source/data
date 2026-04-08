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
    "https://docs.google.com/spreadsheets/d/1lrOMHgE0yx_cS8fALDQrbEDqNW_7BuDVDrwLDYDXmV0/edit?gid=0#gid=0",
    "https://docs.google.com/spreadsheets/d/1FTPSWfc0hNab2-cYqypfF4pyOvwHBkbHVYcOsJwaH24/edit?gid=0#gid=0",
    "https://docs.google.com/spreadsheets/d/18hPOfPcilqGoyIPiXikP5Geb3D0NbDJpTDHQCTAc_Lc/edit?gid=0#gid=0",
]
TARGET_SPREAD_URL = "https://docs.google.com/spreadsheets/d/1PagWGqwt9WhpaCSvopMydXaPlsLSlZ2FrAdQMfMSK1g/edit?gid=0#gid=0"
MULTI_TARGET_SPREAD_URLS = [
    "https://docs.google.com/spreadsheets/d/1hClSA4u_gE5KUudt2Vz2dHJVmh9a3AciCjycjB3Rvds/edit?pli=1&gid=382052561#gid=382052561",
    "https://docs.google.com/spreadsheets/d/1OU786oBbozzYkvr2O9WUG3IECJfLgxVwDOt_lZRsAKo/edit?gid=0#gid=0"
]

# Fixed: Added comma after 'PRODUCT 1'
REQUIRED_COLUMNS = [
    'COUNTRY', 'AGENT', 'DATE', 'TRACKING NUM', 'EM NUMBER', 'NAME', 'NUMBER1', 'PRODUCT 1',
    'VALUE', 'PAYMENT METHOD', 'DELIVERY AGENTS', 'STATUS', 'DISPATCHED DATE', 'PRODUCT 2', 'QTY 2'
]

COLUMN_RENAME_MAP = {
    'TRACKING NUM': 'TRACKING NUM', # This ensures 'TRACKING NUM' becomes 'TRACKING NUM'
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

def clean_duplicate_columns(df):
    cols = pd.Series(df.columns)
    for dup in cols[cols.duplicated()].unique(): 
        cols[cols == dup] = [f"{dup}.{i}" if i != 0 else dup for i in range(sum(cols == dup))]
    df.columns = cols
    return df

def format_date_to_string(df, col_name='DATE'):
    if col_name in df.columns:
        # Convert to datetime objects first to handle multiple input formats
        df[col_name] = pd.to_datetime(df[col_name], errors='coerce', dayfirst=True)
        # Convert back to standardized string YYYY-MM-DD
        df[col_name] = df[col_name].dt.strftime('%Y-%m-%d').replace('NaT', '')
    return df


def run_report_cycle():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Starting update cycle...")
    try:
        c = conf.get_config(file_name=CONFIG_PATH)
        target_sales_spread = Spread(TARGET_SPREAD_URL, config=c)
        
        all_logistic_dfs = []
        for url in LOGISTIC_SOURCES:
            try:
                spread = Spread(url, config=c)
                raw_data = spread.sheet_to_df(sheet="CRM", index=None)
                
                if not raw_data.empty:

                    raw_data = clean_duplicate_columns(raw_data)
                    # standardized = standardize_df(raw_data, fallback_country="KSA")
                    if 'AGENT' in raw_data.columns:
                        raw_data['AGENT'] = raw_data['AGENT'].astype(str).str.upper()

                    if 'Customer Path' in raw_data.columns:
                        raw_data = raw_data[raw_data['Customer Path'].str.strip().str.upper() == 'EMARATH GLOBAL']
                        # Re-standardize after filtering if necessary
                        # standardized = standardize_df(raw_data, fallback_country="KSA")

                    if not raw_data.empty:
                        all_logistic_dfs.append(raw_data)
                        
            except Exception as e:
                print(f" ! Error Logistic URL {url}: {e}")

        if not all_logistic_dfs:
            print("No data found to process.")
            return

        # Combine all standardized dataframes
        df = pd.concat(all_logistic_dfs, ignore_index=True)
        
        # 4. Process Dates
        for col in ['DATE', 'DISPATCHED DATE']:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')

        # 5. Fix TRACKING NUM (Now guaranteed to exist by standardize_df)
        df['TRACKING NUM'] = df['TRACKING NUM'].replace(r'^\s*$', np.nan, regex=True)
        df_clean = df.dropna(subset=['TRACKING NUM']).copy()

        current_month = datetime.now().month
        current_year = datetime.now().year

        # 2. Filter df_clean for rows matching BOTH current month and year
        current_month_df = df_clean[
            (df_clean['DATE'].dt.month == current_month) & 
            (df_clean['DATE'].dt.year == current_year)
        ].copy()

        print(f"Processed {len(current_month_df)} records for month {current_month}.")

        safe_upload(target_sales_spread,'DISPATCH_DATA', current_month_df)
        
        # 6. Main Upload
        print(f"Pushing DISPATCH data to {len(MULTI_TARGET_SPREAD_URLS)} targets...")
        for target_url in MULTI_TARGET_SPREAD_URLS:
            try:
                t_spread = Spread(target_url, config=c)
                # Format dates for human readability before upload
                df_display = df_clean.copy()
                # for col in ['DATE', 'DISPATCHED DATE']:
                #     df_display[col] = df_display[col].dt.strftime('%Y-%m-%d').fillna("")
                
                safe_upload(t_spread, 'DISPATCH', df_display)
                print(f"   - Successfully updated DISPATCH in: {target_url[:45]}...")
            except Exception as e:
                print(f"   ! Failed to upload to {target_url}: {e}")

        # 7. Pending & Aging Logic
        today = pd.Timestamp.now().normalize()
        exclude = ["SALE CLOSED", "DELIVERD/UNPAID", "RTO-ASSIGNED", "CANCELLD&RETURN"]
        
        # Ensure STATUS is string for comparison
        pending_df = df_clean[~df_clean['STATUS'].astype(str).str.upper().isin(exclude)].copy()
        
        # Calculate Age
        pending_df['AGE_DAYS'] = (today - pending_df['DATE']).dt.days

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
            # for col in ['DATE', 'DISPATCHED DATE']:
            #     seg_disp[col] = seg_disp[col].dt.strftime('%Y-%m-%d').fillna("")
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