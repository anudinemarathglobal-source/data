import pandas as pd
import numpy as np
from gspread_pandas import Spread, conf
import math
import time
import sys
import re

# --- 1. CONFIGURATION ---

# --- 1. CONFIGURATION ---
CONFIG_PATH = r'C:\Users\USER\Downloads\Emarath_global\service_account.json'

SOURCE_SHEET_URLS = [
    "https://docs.google.com/spreadsheets/d/1aXobnCl4QQ-hQ_IWoTPo2ZEo3HM5kNwycwMBJtEDOfc/edit",
    "https://docs.google.com/spreadsheets/d/1ztfkIIEeL9EmSdLQIGhBGIFbraMLmqEBwc8FP-CA_3c/edit"
]

LOGISTIC_SHEET_URLS = [
    "https://docs.google.com/spreadsheets/d/1ZccUbe1L-suAl-0aEW6p6lj2j263VwEmk0HRkvjCKKs/edit",
    "https://docs.google.com/spreadsheets/d/1pVBNDiZcjlZnW5-JdsrXsovBr4j7mf9gvrKLlZpfkiU/edit"
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


REQUIRED_COLUMNS = [
    'COUNTRY', 'AGENT', 'DATE', 'TRACKING NUMBER', 'EM NUMBER', 'NAME', 'NUMBER1', 'PRODUCT 1', 'QTY 1', 'PRODUCT 2', 'QTY 2',
    'VALUE', 'PAYMENT METHOD', 'DELIVERY AGENTS', 'STATUS', 'DISPATCHED DATE', 'DELIVERED / RTO DATE'
]

COLUMN_RENAME_MAP = {
    'TRACKING NUM': 'TRACKING NUMBER',
    'EMNUMBER': 'EM NUMBER',
    'DISPATCHED\nDATE': 'DISPATCHED DATE',
}

# --- 2. HELPER FUNCTIONS ---

def clean_column_names(df):
    """Fixes duplicate column names and normalizes them."""
    cols = df.columns.astype(str).str.strip().str.upper()
    cols = [c.replace('\n', ' ') for c in cols]
    
    # Create a new list for deduplicated names
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

def standardize_df(df, fallback_country="UNKNOWN"):
    if df.empty:
        return pd.DataFrame(columns=REQUIRED_COLUMNS)
    
    df = clean_column_names(df)
    
    # Apply renames
    for old_col, new_col in COLUMN_RENAME_MAP.items():
        if old_col.upper() in df.columns:
            df = df.rename(columns={old_col.upper(): new_col})
    
    if 'COUNTRY' in df.columns:
        df['COUNTRY'] = df['COUNTRY'].replace('', np.nan).ffill().bfill()
    else:
        df['COUNTRY'] = fallback_country
    
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            df[col] = np.nan

    df = df[REQUIRED_COLUMNS].copy()
    agent_mask = df['AGENT'].astype(str).str.strip().ne('')
    em_mask = df['EM NUMBER'].astype(str).str.strip().ne('')
    return df[agent_mask | em_mask]

def clean_and_sum_digits(value):
    val_str = str(value).strip()
    if not val_str or val_str.lower() == 'nan':
        return 0
    
    numbers = re.findall(r'\d+', val_str)
    total = 0
    for num in numbers:
        if len(num) > 3 and len(num) % 3 == 0:
            chunks = [int(num[i:i+3]) for i in range(0, len(num), 3)]
            total += sum(chunks)
        else:
            total += int(num)
    return total

def safe_upload(spreadsheet, df, sheet_name):
    if not df.empty:
        try:
            spreadsheet.open_sheet(sheet_name, create=True)
            rows_needed = len(df) + 2
            cols_needed = len(df.columns)
            
            try:
                spreadsheet.sheet.resize(rows=max(100, rows_needed), cols=max(26, cols_needed))
            except Exception as resize_err:
                print(f" Note: Resize skipped/failed for {sheet_name}: {resize_err}")

            spreadsheet.sheet.batch_clear(['A2:Z']) 

            df_upload = df.copy()
            for col in df_upload.columns:
                if 'DATE' in str(col).upper() or df_upload[col].dtype == 'object':
                    df_upload[col] = df_upload[col].astype(str).replace('NaT', '').replace('nan', '')
            
            spreadsheet.df_to_sheet(df_upload, index=False, replace=False, headers=False, start="A2")
            print(f"Successfully updated sheet: {sheet_name}")
            
        except Exception as e:
            print(f"Error uploading to {sheet_name}: {e}")

# --- 3. CORE PIPELINE ---
def run_report_pipeline():
    print(f"\n--- Starting Pipeline Run: {time.ctime()} ---")
    c = conf.get_config(file_name=CONFIG_PATH)
    
    # 3.1. Process Logistics
    all_logi_data = []
    for url in LOGISTIC_SHEET_URLS:
        try:
            spread = Spread(url, config=c)
            raw_data = spread.sheet_to_df(sheet="CRM", index=None)
            if not raw_data.empty:
                standardized = standardize_df(raw_data, fallback_country="KSA")
                all_logi_data.append(standardized)
        except Exception as e:
            print(f" ! Error accessing Logistics URL {url}: {e}")

    if not all_logi_data:
        print("No logistics data found. Creating empty defaults.")
        logi_report = pd.DataFrame(columns=['DATE', 'AGENT', 'ORDER_CONVERTED', 'SALES_AMOUNT'])
        master_Dispatch_Data = pd.DataFrame(columns=REQUIRED_COLUMNS)
    else:
        # Concatenate and handle Index
        logi_df = pd.concat(all_logi_data, ignore_index=True)
        logi_df = logi_df[logi_df['TRACKING NUMBER'].astype(str).str.strip().ne('')].copy()
        logi_df['DATE'] = pd.to_datetime(logi_df['DATE'], errors='coerce', dayfirst=True)
        master_Dispatch_Data = logi_df.copy()
        logi_df['DATE'] = logi_df['DATE'].dt.date
        
        logi_df['CLEAN_TOTAL'] = logi_df['VALUE'].apply(clean_and_sum_digits)
        
        logi_report = logi_df.groupby(['DATE', 'AGENT']).agg(
            ORDER_CONVERTED=('STATUS', 'count'),
            SALES_AMOUNT=('CLEAN_TOTAL', 'sum')
        ).reset_index()
        logi_report['AGENT'] = logi_report['AGENT'].str.strip().str.upper()

    # 3.2. Process BDE Sheets (Source)
    all_bde_data_list = []
    mapping_cols = {
        'DATE': 'DATE', 'AGENT': 'AGENT', 'REF NO':'EM NUMBER', 'COUNTRY': 'COUNTRY', 
        'CUSTOMER PATH': 'CUSTOMER PATH', 'PRODUCT 1': 'PRODUCT', 'NAME': 'NAME', 
        'PHONE NO 1': 'PHONE NO', 'PHONE NO 2': 'PHONE NO 2', 'STATUS': 'STATUS', 'VALUE': 'AMOUNT'
    }

    for url in SOURCE_SHEET_URLS:
        try:
            emarath_spread = Spread(url, config=c)
            for sheet_name in TARGET_BDE_SHEETS:
                try:
                    df = emarath_spread.sheet_to_df(sheet=sheet_name, index=None)
                    if df.empty:
                        continue
                    
                    # Fix duplicate columns before processing
                    df = clean_column_names(df)
                    
                    # Check mapping against uniquely named columns
                    if all(col in df.columns for col in mapping_cols.keys()):
                        subset = df[list(mapping_cols.keys())].copy()
                        subset.rename(columns=mapping_cols, inplace=True)
                        
                        subset['DATE'] = pd.to_datetime(subset['DATE'], errors='coerce', dayfirst=True)
                        subset = subset.dropna(subset=['DATE'])
                        subset['DATE'] = subset['DATE'].dt.date
                        
                        # Data cleaning
                        subset = subset[~subset['AGENT'].astype(str).str.strip().str.upper().isin(['', 'NAN', 'NONE', 'N/A', 'UNKNOWN', '0'])]
                        subset = subset[~subset['PHONE NO'].astype(str).str.strip().str.upper().isin(['', 'NAN', 'NONE', 'N/A', 'UNKNOWN', '0'])]
                        
                        subset['AMOUNT'] = pd.to_numeric(subset['AMOUNT'], errors='coerce').fillna(0)
                        subset['AMOUNT'] = subset['AMOUNT'].apply(clean_and_sum_digits)
                        
                        for col in ['CUSTOMER PATH', 'STATUS', 'AGENT']:
                            subset[col] = subset[col].astype(str).str.strip().str.upper()
                        
                        all_bde_data_list.append(subset)
                except Exception as sheet_err:
                    continue # Skip problematic individual sheets
        except Exception as e:
            print(f"Error accessing Source URL {url}: {e}")

    if not all_bde_data_list:
        print("No BDE data found. Ending pipeline.")
        return

    # 3.3. Processing Leads
    lead_df = pd.concat(all_bde_data_list, ignore_index=True).reset_index(drop=True)
    lead_df = lead_df.sort_values(by='DATE', ascending=True)
    Master_Lead_Data = lead_df.copy()
    
    invalid_phones = ['0', 'NAN', 'NONE', '', 'nan']
    lead_df['is_contacted'] = (lead_df['CUSTOMER PATH'] == 'LEAD') & (~lead_df['PHONE NO'].astype(str).str.strip().isin(invalid_phones))
    lead_df['is_pending'] = (lead_df['CUSTOMER PATH'] == 'LEAD') & (lead_df['PHONE NO'].astype(str).str.strip().isin(invalid_phones))
    lead_df['is_followup_conv'] = (lead_df['CUSTOMER PATH'] == 'MISSED LEAD') & (lead_df['STATUS'] == 'WON')

    report_data = lead_df.groupby(['DATE', 'AGENT']).agg(
        TOTAL_LEAD=('CUSTOMER PATH', lambda x: (x == 'LEAD').sum()),
        CONTACTED=('is_contacted', 'sum'),
        PENDING=('is_pending', 'sum'),
        FOLLOW_UP_LEADS=('CUSTOMER PATH', lambda x: (x == 'MISSED LEAD').sum()),
        FOLLOW_UP_LEADS_CONVERTED=('is_followup_conv', 'sum'),
    ).reset_index()
    report_data['AGENT'] = report_data['AGENT'].str.strip().str.upper()

    # 3.4. Final Merge
    merged_df = report_data.merge(logi_report, on=['DATE', 'AGENT'], how='left').fillna(0)
    
    # Weekly Summary
    merged_week_df = merged_df.copy()
    merged_week_df['WEEK'] = pd.to_datetime(merged_week_df['DATE']).apply(lambda x: f"WEEK {math.ceil(x.day / 7)}")
    
    weekly_summary = merged_week_df.groupby(['WEEK', 'AGENT']).agg({
        'DATE': lambda x: f"{x.min().strftime('%d/%m')} - {x.max().strftime('%d/%m')}",
        'TOTAL_LEAD': 'sum',
        'CONTACTED': 'sum',
        'ORDER_CONVERTED': 'sum',
        'PENDING': 'sum',
        'FOLLOW_UP_LEADS': 'sum',
        'FOLLOW_UP_LEADS_CONVERTED': 'sum',
        'SALES_AMOUNT': 'sum'
    }).reset_index().rename(columns={'DATE': 'DATE_RANGE'})

    # 3.5. Upload
    target_spread = Spread(TARGET_SHEET_URL, config=c)
    safe_upload(target_spread, master_Dispatch_Data, 'Master_Dispatch_Data')
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