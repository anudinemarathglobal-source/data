import pandas as pd
import numpy as np
from datetime import datetime
import time
import os
import traceback
import re
from gspread_pandas import Spread, conf

# ------------------------------------ CONFIGURATION ---------------------------------------------------------
CONFIG_PATH = r'C:\Users\USER\Downloads\Emarath_global\service_account.json'
SOURCE_SHEET_URL = "https://docs.google.com/spreadsheets/d/1PagWGqwt9WhpaCSvopMydXaPlsLSlZ2FrAdQMfMSK1g/edit"
OUTPUT_DIR = './Output_Data'

# TARGET SHEETS ARRAY
TARGET_SHEETS = [
    {
        "name": "LOGISTIC",
        "url": "https://docs.google.com/spreadsheets/d/19o30vQsd8e6W7LJhBjH9NFopGVgRN9Mfiqau1GkWH5Q/edit",
        "tab": "WON_DATA (APRIL)"
    },
    {
        "name": "ALL DATA DASHBOARD",
        "url": "https://docs.google.com/spreadsheets/d/1X5ayTQNLratOzsFNiCEkvegDJ645gaMWoNbz1fEThA0/edit",
        "tab": "WON_DATA (APRIL)"
    },
    # {
    #     "name": "BDM Dashboard",
    #     "url": "https://docs.google.com/spreadsheets/d/1OU786oBbozzYkvr2O9WUG3IECJfLgxVwDOt_lZRsAKo/edit",
    #     "tab": "WON_DATA (MAR)"
    # }
]

INTERVAL_MINUTES = 10

# ------------------------------------------------------------------------------------------------------------

def clean_columns(df):
    cols = pd.Series(df.columns).fillna("unnamed_col")
    if not cols.is_unique:
        counts = {}
        new_cols = []
        for col in cols:
            if col in counts:
                counts[col] += 1
                new_cols.append(f"{col}_{counts[col]}")
            else:
                counts[col] = 0
                new_cols.append(col)
        df.columns = new_cols
    return df

# ------------------------------------------------------------------------------------------------------------

def safe_upload_to_targets(master_df, config):
    for target in TARGET_SHEETS:
        try:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Syncing to {target['name']}...")
            spreadsheet = Spread(target['url'], config=config)
            
            spreadsheet.open_sheet(target['tab'], create=True)
            
            needed_rows = len(master_df) + 10
            needed_cols = len(master_df.columns) + 2
            
            curr_rows = spreadsheet.sheet.row_count
            curr_cols = spreadsheet.sheet.col_count

            if curr_rows < needed_rows or curr_cols < needed_cols:
                spreadsheet.sheet.resize(rows=max(curr_rows, needed_rows), 
                                         cols=max(curr_cols, needed_cols))

            spreadsheet.sheet.batch_clear([f'A2:Z{spreadsheet.sheet.row_count}']) 
        
            upload_df = master_df.astype(str)
            spreadsheet.df_to_sheet(upload_df, index=False, replace=False, headers=False, start="A2")
            # spreadsheet.df_to_sheet(master_df, index=False, replace=True, sheet=target['tab'])
            print(f"Successfully updated {target['name']} / {target['tab']}")
            
        except Exception as upload_error:
            print(f"Error uploading to {target['name']}: {upload_error}")

# ------------------------------------------------------------------------------------------------------------

def run_won_analysis():
    try:
        now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"\n[{now_ts}] Starting sync cycle...")

        if not os.path.exists(OUTPUT_DIR):
            os.makedirs(OUTPUT_DIR)

        if not os.path.exists(CONFIG_PATH):
            raise FileNotFoundError(f"Service account file not found at: {CONFIG_PATH}")

        # Connection
        c = conf.get_config(file_name=CONFIG_PATH)
        spread_source = Spread(SOURCE_SHEET_URL, config=c)
        spread_source.value_render_option = 'FORMATTED_VALUE'

        all_won_data = []
        skip_sheets = ["MAIN DASHBOARD", "CRM", "Dropdowns", "Sheet1", "DASHBOARD", "20", "Customer complaint form ", "Pivot Table 43",'REPLACEMENT & REFUND']

        # Loop through all Agent Sheets in Source
        for sheet in spread_source.sheets:
            sheet_name = sheet.title
            if sheet_name in skip_sheets:
                continue
            
            try:
                df = spread_source.sheet_to_df(sheet=sheet_name, index=None, header_rows=1)
                if df.empty:
                    continue

                df = clean_columns(df)
                
                # Look for Status column (case insensitive)
                status_col = next((c for c in df.columns if str(c).upper() == 'STATUS'), None)
                
                if status_col:
                    won_df = df[df[status_col].astype(str).str.upper().str.strip() == "WON"].copy()
                    
                    if not won_df.empty:
                        won_df['Source_Agent'] = sheet_name
                        all_won_data.append(won_df)
                else:
                    print(f"! Skip: '{sheet_name}' - No 'STATUS' column found.")

            except Exception as e:
                print(f"   ! Error processing sheet '{sheet_name}': {e}")

        # Process and Clean Consolidated Data
        if all_won_data:
            master_won_df = pd.concat(all_won_data, ignore_index=True, sort=False)

            # Cleanup unnecessary columns
            cols_to_remove = ['1', '2', '3', '4', 'GENDER', '', 'unnamed_col']
            existing_cols_to_drop = [c for c in cols_to_remove if c in master_won_df.columns]
            master_won_df = master_won_df.drop(columns=existing_cols_to_drop)
            master_won_df = master_won_df.loc[:, ~master_won_df.columns.duplicated()]

            # Sort by Date
            date_col = next((c for c in master_won_df.columns if str(c).upper() == 'DATE'), None)
            if date_col:
                master_won_df = master_won_df.sort_values(by=[date_col, 'Source_Agent'], ascending=[True, False])

            safe_upload_to_targets(master_won_df, c)

            file_path = os.path.join(OUTPUT_DIR, 'won_data_backup_MAR.xlsx')
            master_won_df.to_excel(file_path, index=False)
            
            print(f"--- SUCCESS: Updated {len(master_won_df)} rows at {datetime.now().strftime('%H:%M:%S')} ---")
        else:
            print("No 'WON' status rows found across any sheets.")

    except Exception as e:
        print(f"CRITICAL ERROR: {str(e)}")
        traceback.print_exc() 

# ------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    print(f"Scheduler started. Syncing every {INTERVAL_MINUTES} minutes.")
    try:
        while True:
            run_won_analysis()
            print(f"Waiting {INTERVAL_MINUTES} minutes for next run...")
            time.sleep(INTERVAL_MINUTES * 60) 
            
    except KeyboardInterrupt:
        print("\nScheduler stopped successfully.")