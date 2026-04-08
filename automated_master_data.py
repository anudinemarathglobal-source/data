import pandas as pd
import numpy as np
from gspread_pandas import Spread, conf
import math
import time
import sys
import re

# --- 1. CONFIGURATION ---
CONFIG_PATH = r'C:\Users\USER\Downloads\Emarath_global\service_account.json'

# Unified source list (The error was a missing comma here previously)
SOURCE_SHEET_URLS = [
    "https://docs.google.com/spreadsheets/d/1aXobnCl4QQ-hQ_IWoTPo2ZEo3HM5kNwycwMBJtEDOfc/edit",
    "https://docs.google.com/spreadsheets/d/1ztfkIIEeL9EmSdLQIGhBGIFbraMLmqEBwc8FP-CA_3c/edit", 
    "https://docs.google.com/spreadsheets/d/1PagWGqwt9WhpaCSvopMydXaPlsLSlZ2FrAdQMfMSK1g/edit",
]

LOGISTIC_SHEET_URLS = [
    "https://docs.google.com/spreadsheets/d/1lrOMHgE0yx_cS8fALDQrbEDqNW_7BuDVDrwLDYDXmV0/edit?gid=0#gid=0",
    "https://docs.google.com/spreadsheets/d/1FTPSWfc0hNab2-cYqypfF4pyOvwHBkbHVYcOsJwaH24/edit?gid=0#gid=0",
    "https://docs.google.com/spreadsheets/d/18hPOfPcilqGoyIPiXikP5Geb3D0NbDJpTDHQCTAc_Lc/edit?gid=0#gid=0",
]

TARGET_SHEET_URL = "https://docs.google.com/spreadsheets/d/1IeqrQngvF8fZCwMyXzAR4J1dBkK02RRDtep2D1-yCe0/edit"

TARGET_BDE_SHEETS = [
    'RAHIB', 'HABIYA', 'BURHANA', 'SHAMNA', 'ARUN',
    'CHAITHANYA', 'ZAKIYA', 'SAFAN', 'SUSHANTHIKA',
    'ADWAITHA', 'NEHA', 'GOWTHAM', 'AMINA', 'NAFI',
    'RINSIYA', 'ARSHAD', "SHIHAD", "RAHIYAD", "AKASH", 
    "NAJIYA", "RANJITH", "AFNAN", "NIHAD", "SHYAMJIL", 
    'ADHIL', 'NAJA', 'AJIN', 'SHABNA', 'FARSANA'
]

COLUMN_RENAME_MAP = {
    'TRACKING NUM': 'TRACKING NUMBER',
    'EMNUMBER': 'EM NUMBER',
    'DISPATCHED\nDATE': 'DISPATCHED DATE',
    'REF NO': 'EM NUMBER'
}

# --- 2. HELPER FUNCTIONS ---

def clean_column_names(df):
    """Fixes duplicate column names and normalizes them."""
    cols = df.columns.astype(str).str.strip().str.upper()
    cols = [c.replace('\n', ' ') for c in cols]
    
    new_cols = []
    counts = {}
    for col in cols:
        if col in counts:
            counts[col] += 1
            new_cols.append(f"{col}.{counts[col]}")
        else:
            counts[col] = 0
            new_cols.append(col)
            
    df.columns = new_cols
    return df

def clean_and_sum_digits(value):
    """Extracts numbers from messy string values and sums them."""
    val_str = str(value).strip()
    if not val_str or val_str.lower() == 'nan':
        return 0
    
    numbers = re.findall(r'\d+', val_str)
    total = 0
    for num in numbers:
        # Handle specific formatting logic provided in original script
        if len(num) > 3 and len(num) % 3 == 0:
            chunks = [int(num[i:i+3]) for i in range(0, len(num), 3)]
            total += sum(chunks)
        else:
            try:
                total += int(num)
            except:
                continue
    return total

def safe_upload(spreadsheet, df, sheet_name):
    """Handles the upload process safely to Google Sheets."""
    if df is not None and not df.empty:
        try:
            # Open or create the sheet
            spreadsheet.open_sheet(sheet_name, create=True)
            
            # Clear existing data before upload
            spreadsheet.sheet.clear() 

            # Pre-process for GSheets compatibility (convert dates/nans to string)
            df_upload = df.copy()
            for col in df_upload.columns:
                if 'DATE' in str(col).upper() or df_upload[col].dtype == 'object':
                    df_upload[col] = df_upload[col].astype(str).replace(['NaT', 'nan', 'None', '<NA>'], '')
            
            # Upload
            spreadsheet.df_to_sheet(df_upload, index=False, replace=False, headers=True, start="A1")
            print(f"Successfully updated sheet: {sheet_name}")
            
        except Exception as e:
            print(f"Error uploading to {sheet_name}: {e}")

# --- 3. CORE PIPELINE ---

def run_report_pipeline():
    print(f"\n--- Starting Pipeline Run: {time.ctime()} ---")
    try:
        c = conf.get_config(file_name=CONFIG_PATH)
    except Exception as e:
        print(f"Critical Error: Could not load config file at {CONFIG_PATH}. {e}")
        return

    # 3.1. Process Logistics (Master Dispatch Data)
    all_logi_data = []
    for url in LOGISTIC_SHEET_URLS:
        try:
            spread = Spread(url, config=c)
            # Try to get the CRM sheet
            raw_data = spread.sheet_to_df(sheet="CRM", index=None)
            
            if not raw_data.empty:
                raw_data = clean_column_names(raw_data)
                raw_data.rename(columns=COLUMN_RENAME_MAP, inplace=True)
                
                if 'AGENT' in raw_data.columns:
                    raw_data['AGENT'] = raw_data['AGENT'].astype(str).str.upper().str.strip()

                if 'CUSTOMER PATH' in raw_data.columns:
                        raw_data = raw_data[raw_data['CUSTOMER PATH'].str.strip().str.upper() == 'EMARATH GLOBAL']
                all_logi_data.append(raw_data)        
                        
        except Exception as e:
            print(f" ! Warning: Error processing CRM in URL {url}: {e}")      

    if not all_logi_data:
        print("No logistics data found.")
        master_Dispatch_Data = pd.DataFrame()
        logi_report = pd.DataFrame(columns=['DATE', 'AGENT', 'ORDER_CONVERTED', 'SALES_AMOUNT'])
    else:
        Dispatch_Data = pd.concat(all_logi_data, ignore_index=True, sort=False)
        master_Dispatch_Data = Dispatch_Data.copy()
        # Report Logic
        logi_calc = Dispatch_Data.copy()
        logi_calc['DATE'] = pd.to_datetime(logi_calc['DATE'], errors='coerce', dayfirst=True)
        logi_calc = logi_calc.dropna(subset=['DATE'])
        logi_calc['DATE'] = logi_calc['DATE'].dt.date
        
        # Determine value column
        val_col = 'VALUE' if 'VALUE' in logi_calc.columns else 'AMOUNT'
        if val_col in logi_calc.columns:
            logi_calc['CLEAN_TOTAL'] = logi_calc[val_col].apply(clean_and_sum_digits)
        else:
            logi_calc['CLEAN_TOTAL'] = 0

        logi_report = logi_calc.groupby(['DATE', 'AGENT']).agg(
            ORDER_CONVERTED=('STATUS', 'count'),
            SALES_AMOUNT=('CLEAN_TOTAL', 'sum')
        ).reset_index()

    # 3.2. Process BDE Sheets (Master Lead Data)
    all_bde_data_list = []
    for url in SOURCE_SHEET_URLS:
        try:
            emarath_spread = Spread(url, config=c)
            available_sheets = [s.title for s in emarath_spread.sheets]
            
            for sheet_name in TARGET_BDE_SHEETS:
                if sheet_name not in available_sheets:
                    continue
                try:
                    df = emarath_spread.sheet_to_df(sheet=sheet_name, index=None)
                    if df.empty: continue
                    
                    df = clean_column_names(df)
                    df.rename(columns=COLUMN_RENAME_MAP, inplace=True)
                    df['SOURCE_SHEET'] = sheet_name
                    all_bde_data_list.append(df)
                except:
                    continue 
        except Exception as e:
            print(f"Error accessing Source URL {url}: {e}")

    if not all_bde_data_list:
        print("No BDE data found.")
        return

    # 3.3. Processing Leads
    Master_Lead_Data = pd.concat(all_bde_data_list, ignore_index=True, sort=False)
    calc_df = Master_Lead_Data.copy()
    
    if 'DATE' in calc_df.columns:
        calc_df['DATE_DT'] = pd.to_datetime(calc_df['DATE'], errors='coerce', dayfirst=True)
        calc_df = calc_df.dropna(subset=['DATE_DT'])
        calc_df['DATE_ONLY'] = calc_df['DATE_DT'].dt.date
    
    path_col = 'CUSTOMER PATH' if 'CUSTOMER PATH' in calc_df.columns else 'PATH'
    phone_col = 'PHONE NO' if 'PHONE NO' in calc_df.columns else 'PHONE NO 1'
    status_col = 'STATUS'

    invalid_phones = ['0', 'NAN', 'NONE', '', 'nan']
    
    calc_df['is_contacted'] = (calc_df[path_col].astype(str).str.upper() == 'LEAD') & (~calc_df[phone_col].astype(str).str.strip().isin(invalid_phones))
    calc_df['is_pending'] = (calc_df[path_col].astype(str).str.upper() == 'LEAD') & (calc_df[phone_col].astype(str).str.strip().isin(invalid_phones))
    calc_df['is_followup_conv'] = (calc_df[path_col].astype(str).str.upper() == 'MISSED LEAD') & (calc_df[status_col].astype(str).str.upper() == 'WON')

    report_data = calc_df.groupby(['DATE_ONLY', 'AGENT']).agg(
        TOTAL_LEAD=(path_col, lambda x: (x.astype(str).str.upper() == 'LEAD').sum()),
        CONTACTED=('is_contacted', 'sum'),
        PENDING=('is_pending', 'sum'),
        FOLLOW_UP_LEADS=(path_col, lambda x: (x.astype(str).str.upper() == 'MISSED LEAD').sum()),
        FOLLOW_UP_LEADS_CONVERTED=('is_followup_conv', 'sum'),
    ).reset_index().rename(columns={'DATE_ONLY': 'DATE'})

    # 3.4. Final Merge & Weekly Summary
    report_data['AGENT'] = report_data['AGENT'].astype(str).str.strip().str.upper()
    merged_df = report_data.merge(logi_report, on=['DATE', 'AGENT'], how='left').fillna(0)
    
    merged_week_df = merged_df.copy()
    merged_week_df['WEEK'] = pd.to_datetime(merged_week_df['DATE']).apply(lambda x: f"WEEK {math.ceil(x.day / 7)}")
    
    weekly_summary = merged_week_df.groupby(['WEEK', 'AGENT']).agg({
        'DATE': lambda x: f"{pd.to_datetime(x).min().strftime('%d/%m')} - {pd.to_datetime(x).max().strftime('%d/%m')}",
        'TOTAL_LEAD': 'sum',
        'CONTACTED': 'sum',
        'ORDER_CONVERTED': 'sum',
        'PENDING': 'sum',
        'FOLLOW_UP_LEADS': 'sum',
        'FOLLOW_UP_LEADS_CONVERTED': 'sum',
        'SALES_AMOUNT': 'sum'
    }).reset_index().rename(columns={'DATE': 'DATE_RANGE'})

    # 3.5. Final Uploads to Target Spreadsheet
    target_spread = Spread(TARGET_SHEET_URL, config=c)
    safe_upload(target_spread, master_Dispatch_Data, 'master_Dispatch_Data')
    safe_upload(target_spread, Master_Lead_Data, 'Master_Lead_Data')
    safe_upload(target_spread, merged_df, 'Daily_Report')
    safe_upload(target_spread, weekly_summary, 'Weekly_Summary_Report')
    
    print("--- Pipeline Finished Successfully ---")

if __name__ == "__main__":
    try:
        while True:
            run_report_pipeline()
            print("Sleeping for 1 hour...")
            time.sleep(3600)
    except KeyboardInterrupt:
        print("\nShutdown complete.")