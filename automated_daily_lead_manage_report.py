import pandas as pd
import numpy as np
from gspread_pandas import Spread, conf
import datetime
import time
import schedule
import sys
import os

# --- CONFIGURATION ---
CONFIG_PATH = r'C:\Users\USER\Downloads\Emarath_global\service_account.json'
DASHBOARD_SHEET_URL = "https://docs.google.com/spreadsheets/d/1PagWGqwt9WhpaCSvopMydXaPlsLSlZ2FrAdQMfMSK1g/edit"
TARGET_SHEET_URL = "https://docs.google.com/spreadsheets/d/1PqMNtFU0bas_BGYFhIYWojO2petW-TOGxeRB2OWnF08/edit?pli=1&gid=526013825#gid=526013825"

# TARGET_BDE_SHEETS = [
#     'RAHIB', 'HABIYA', 'BURHANA', 'SHAMNA', 'ARUN',
#     'CHAITHANYA', 'ZAKIYA', 'SAFAN',  
#     'ADWAITHA', 'NEHA', 'GOWTHAM', 'AMINA', 'RINSIYA', 'ARSHAD',  
#     "SHIHAD", "RAHIYAD", "AKASH", "NAJIYA",
#     # "SHYAMJIL", 'ADHIL', "HIBA", 'NAJA', 'AJIN', 'SHABNA', 'FARSANA', 'SUSHANTHIKA', 'NAFI',
# ]

TARGET_BDE_SHEETS = [
    'RAHIB', 'HABIYA', 'SHAMNA', 
    'CHAITHANYA', 'ZAKIYA', 'SAFAN', 'RANJITH',
    'ADWAITHA', 'NEHA',  'AMINA', 'ARSHAD', "SHIHAD", "RAHIYAD", "AKASH", "NAJIYA", "AFNAN", "NIHAD"
    # "SHYAMJIL", 'ADHIL', "HIBA", 'NAJA', 'AJIN', 'SHABNA', 'FARSANA', 'SUSHANTHIKA', 'NAFI', 'AJIN', 'SHABNA', 'FARSANA', 'BURHANA', 'ARUN', 'RINSIYA', 'GOWTHAM',
]

LEAD_STATUSES = ["WON", "SUPER HOT", "HOT", "WARM", "COLD", "BOOKING", "WHATS APP ENGAGE"]

def safe_upload(spreadsheet, df, sheet_name):
    if not df.empty:
        try:
            spreadsheet.open_sheet(sheet_name, create=True)
            spreadsheet.sheet.batch_clear(['A2:Z1000']) 
            time.sleep(1) 
            upload_df = df.astype(str).replace('nan', '0').replace('NaT', '')
            for attempt in range(3):
                try:
                    spreadsheet.df_to_sheet(upload_df, index=False, replace=False, headers=False, start="A2")
                    print(f"✅ Report successfully uploaded to {sheet_name}")
                    return
                except Exception as e:
                    print(f"⚠️ Upload attempt {attempt+1} failed ({e}). Retrying in 5s...")
                    time.sleep(5)
        except Exception as e:
            print(f"❌ Critical upload error: {e}")

def generate_report():
    today = datetime.datetime.now()
    print(f"[{today.strftime('%Y-%m-%d %H:%M:%S')}] Starting report generation...")
    
    try:
        c = conf.get_config(file_name=CONFIG_PATH)
        emarath_spread = Spread(DASHBOARD_SHEET_URL, config=c)
        target_spread = Spread(TARGET_SHEET_URL, config=c)
    except Exception as e:
        print(f"Failed to connect to Google Sheets: {e}")
        return

    all_data_list = []

    for sheet_name in TARGET_BDE_SHEETS:
        try:
            df = emarath_spread.sheet_to_df(sheet=sheet_name, index=None, header_rows=1)
            
            mapping_cols = {
                'AGENT': 'AGENT', 'COUNTRY': 'REGION', 'CUSTOMER PATH': 'CUSTOMER PATH',
                'PRODUCT 1': 'PRODUCT', 'NAME': 'NAME', 'PHONE NO 1': 'PHONE NO',
                'STATUS': 'STATUS', 'DATE': 'DATE', 'CALL STATUS': 'CALL STATUS'
            }
            
            if not df.empty and all(col in df.columns for col in mapping_cols.keys()):
                subset = df[list(mapping_cols.keys())].copy()
                subset.rename(columns=mapping_cols, inplace=True)

                subset['DATE'] = pd.to_datetime(subset['DATE'], errors='coerce', dayfirst=True)
                subset = subset[subset['DATE'].dt.date == today.date()]

                if subset.empty: continue

                subset['CUSTOMER PATH'] = subset['CUSTOMER PATH'].astype(str).str.strip().str.upper()
                subset = subset[subset['CUSTOMER PATH'] == 'LEAD']
                subset = subset[~subset['AGENT'].astype(str).str.strip().str.upper().isin(['', 'NAN', 'NONE', 'N/A', 'UNKNOWN', '0'])]
                subset = subset[~subset['PHONE NO'].astype(str).str.strip().str.upper().isin(['', 'NAN', 'NONE', 'N/A', 'UNKNOWN', '0'])]
                
                for col in ['PHONE NO', 'CUSTOMER PATH', 'CALL STATUS', 'STATUS']:
                    subset[col] = subset[col].astype(str).str.strip().str.upper()

                all_data_list.append(subset)
                    
        except Exception as e:
            print(f"Error in sheet {sheet_name}: {e}")

    if all_data_list:
        master_df = pd.concat(all_data_list, ignore_index=True)
        master_df['is_unattended'] = ((master_df['CALL STATUS'] == '') | (master_df['CALL STATUS'] == 'NAN'))

        total_leads = master_df.groupby('AGENT')['PHONE NO'].count().to_frame('TOTAL LANDED LEADS')
        unattended = master_df[master_df['is_unattended']].groupby('AGENT').size().to_frame('UNATTENDED')

        status_pivot = master_df.pivot_table(index='AGENT', columns='STATUS', aggfunc='size', fill_value=0)
        
        for status in LEAD_STATUSES:
            if status not in status_pivot.columns:
                status_pivot[status] = 0

        # Join
        final_report = total_leads.join(unattended, how='left').join(status_pivot, how='left').fillna(0).astype(int)

        # Organize Columns
        ordered_cols = ['TOTAL LANDED LEADS'] + LEAD_STATUSES + ['UNATTENDED']
        final_report = final_report.reindex(columns=ordered_cols, fill_value=0)
        final_report = final_report.sort_index()

        # Calculate TOTAL row before adding non-numeric columns
        total_sum = final_report.sum(numeric_only=True)
        final_report.loc['TOTAL'] = total_sum

        # Final Formatting
        final_report.reset_index(inplace=True)
        final_report.rename(columns={'index': 'AGENT'}, inplace=True)
        final_report.insert(0, 'Report Date', today.strftime('%Y-%m-%d'))

        final_report.iloc[-1, final_report.columns.get_loc('AGENT')] = 'TOTAL'

        safe_upload(target_spread, final_report, "Daily_Lead_Management_Report")
    else:
        print(f"No valid records found for {today.date()}.")

    
if __name__ == "__main__":
    print("Lead Management Report Automation Script started")
    try:
        while True:
            try:
                generate_report()
            except Exception as e:
                print(f"Pipeline crashed: {e}")
            
            print("Waiting 5 minutes for the next run...")
            time.sleep(300)  
            
    except KeyboardInterrupt:
        print("\nShutdown signal received. Exiting gracefully.")
        sys.exit(0) 
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1) 
