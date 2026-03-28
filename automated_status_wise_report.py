import pandas as pd
import numpy as np
from datetime import datetime
from gspread_pandas import Spread, conf
import time
import sys

# ----- CONFIGURATION -----
CONFIG_PATH = r'C:\Users\USER\Downloads\Emarath_global\service_account.json'

# ADD NEW MONTHLY URLs HERE
LOGISTIC_SOURCES = [
    # "https://docs.google.com/spreadsheets/d/1S6WaI1rXuf5ogN44ph8weA_z_bMCg85U3uaZ7RHvpfM/edit", # Feb
    "https://docs.google.com/spreadsheets/d/1S6WaI1rXuf5ogN44ph8weA_z_bMCg85U3uaZ7RHvpfM/edit", # Mar
]

TARGET_SHEET_URL = "https://docs.google.com/spreadsheets/d/1X5ayTQNLratOzsFNiCEkvegDJ645gaMWoNbz1fEThA0/edit?gid=918927116#gid=918927116"

# Standardizing Column Names
REQUIRED_COLUMNS = [
    'COUNTRY', 'AGENT', 'DATE', 'TRACKING NUMBER', 'EM NUMBER',
    'NAME', 'NUMBER1', 'NUMBER2', 'STATE / CITY', 'ADDRESS',
    'CUSTOMER PATH', 'STATUS', 'DISPATCHED DATE', 'REASON',
    'DELIVERY AGENT', 'PRODUCT1','QTY', 'PRODUCT2', 'QTY2', 'TOTAL', "PAYMENT METHOD", "Delivered/Cancelled Date"
]

COLUMN_RENAME_MAP = {
    'TRACKING \nNUMBER': 'TRACKING NUMBER',
    'EMNUMBER': 'EM NUMBER',
    'CUSTOMER\nPATH': 'CUSTOMER PATH',
    'DISPATCHED\nDATE': 'DISPATCHED DATE',
    'NATIONAL \nCODE': 'NATIONAL CODE',
    # 'DELIVERY \nAGENT \n(AVAILABLE)' : 'DELIVERY AGENT',
}

def format_dates(df_input, cols):
    df_out = df_input.copy()
    for col in cols:
        if col in df_out.columns:
            df_out[col] = pd.to_datetime(df_out[col], dayfirst=True, errors='coerce')
            df_out[col] = df_out[col].dt.strftime('%m/%d/%Y').where(df_out[col].notna(), other="")
    return df_out

def safe_upload(spreadsheet, df, sheet_name):
    if not df.empty:
        spreadsheet.open_sheet(sheet_name, create=True)
        needed_rows = len(df) + 50
        needed_cols = len(df.columns) + 10

        current_rows = spreadsheet.sheet.row_count
        current_cols = spreadsheet.sheet.col_count

        if current_rows < needed_rows or current_cols < needed_cols:
            spreadsheet.sheet.resize(rows=max(current_rows, needed_rows),
                                     cols=max(current_cols, needed_cols))

        spreadsheet.sheet.batch_clear([f'A2:Z{spreadsheet.sheet.row_count}'])
        upload_df = df.astype(str)
        spreadsheet.df_to_sheet(upload_df, index=False, replace=False, headers=False, start="A2")


def load_and_standardize(spread, sheet_name, country_value):
    try:
        df = spread.sheet_to_df(sheet=sheet_name, index=None)
        df.columns = df.columns.astype(str).str.strip()
        df = df.loc[:, ~df.columns.duplicated()]
        df = df.rename(columns=COLUMN_RENAME_MAP)

        # Standardize to Uppercase for easier matching
        df.columns = df.columns.str.upper()

        df['COUNTRY'] = country_value

        for col in REQUIRED_COLUMNS:
            if col not in df.columns:
                df[col] = ''

        df = df[REQUIRED_COLUMNS]
        df = df[
            df['AGENT'].astype(str).str.strip().ne('') &
            df['EM NUMBER'].astype(str).str.strip().ne('') &
            df['PRODUCT1'].astype(str).str.strip().ne('')
        ]
        return df
    except Exception:
        return pd.DataFrame()

def run_sync():
    try:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting sync...")
        c = conf.get_config(file_name=CONFIG_PATH)
        target_spread = Spread(TARGET_SHEET_URL, config=c)

        all_dataframes = []
        # 1. Consolidate data from all sources
        for url in LOGISTIC_SOURCES:
            spread = Spread(url, config=c)
            ksa     = load_and_standardize(spread, "ORDER LIST - KSA",     "KSA")
            uae     = load_and_standardize(spread, "ORDER LIST - UAE",     "UAE")
            qatar   = load_and_standardize(spread, "ORDER LIST - QATAR",   "QATAR")
            bahrain = load_and_standardize(spread, "ORDER LIST - BAHRAIN", "BAHRAIN")
            all_dataframes.extend([ksa, uae, qatar, bahrain])

        df_sheet = pd.concat(all_dataframes, ignore_index=True)
        # df_sheet.to_excel("./df_sheet.xlsx")
        if df_sheet.empty:
            print("No data found in any sources. Skipping.")
            return

        # FIX: .upper() → .str.upper() (Series has no bare .upper() method)
        df_sheet['STATUS'] = df_sheet['STATUS'].astype(str).str.strip().str.upper()
        df_sheet['DELIVERY AGENT'] = df_sheet['DELIVERY AGENT'].astype(str).str.strip()
        df_sheet = df_sheet.sort_values('DATE', ascending=True)

        if not df_sheet.empty:
            safe_upload(target_spread, df_sheet, "ALL_ORDERS")

        # 2. Process Status Tabs
        statuses = [
            'Delivered and unpaid', 'Order Confirmed', 'Rejected', 'Cancelled',
            'RTO', 'Back to TWS', 'Out for delivery', 'No answer', 'On hold', 'Delivered'
        ]

        status_col_upper = df_sheet['STATUS'].fillna('').astype(str).str.upper().str.strip()

        for status in statuses:
            status_up = status.upper().strip()
            filtered_df = df_sheet[status_col_upper == status_up]

            if not filtered_df.empty:
                tab_name = status_up.replace(' ', '_')[:31]
                safe_upload(target_spread, filtered_df, tab_name)

        # 3. Process Financial Data
        df_clean = df_sheet.dropna(subset=['TRACKING NUMBER']).copy()
        df_clean = df_clean[df_clean['TRACKING NUMBER'].astype(str).str.strip() != ""]

        # date_cols = ['DATE', 'DISPATCHED DATE']
        # date_cols = ['DATE']
        # combined_df = format_dates(df_clean, date_cols)
        combined_df = df_clean.copy()
        combined_df = combined_df.sort_values('DISPATCHED DATE', ascending=True)

        if not combined_df.empty:
            safe_upload(target_spread, combined_df, "DISPATCH_ORDERS")
            

        # Process Pending Statuses
        pending_df = combined_df[~combined_df['STATUS'].isin(['RTO', 'DELIVERED AND UNPAID', 'CANCELLED'])]
        if not pending_df.empty:
            safe_upload(target_spread, pending_df, "PENDINGS")

        print(f"[{datetime.now().strftime('%H:%M:%S')}] Sync completed successfully.")

    except Exception as e:
        print(f"CRITICAL ERROR: {e}")

if __name__ == "__main__":
    print("Multi-Source Scheduler Active.")
    try:
        while True:
            run_sync()
            print(f"Sleeping for 10 minutes...")
            # time.sleep(600)
            time.sleep(60)
    except KeyboardInterrupt:
        print("\nShutdown requested.")
        sys.exit(0)