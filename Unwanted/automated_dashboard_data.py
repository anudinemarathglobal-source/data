import pandas as pd
import numpy as np
import time
import sys
import warnings
from datetime import datetime
from gspread_pandas import Spread, conf

warnings.filterwarnings('ignore', category=DeprecationWarning)

# --- CONFIGURATION ---
CONFIG_PATH = r'C:\Users\USER\Downloads\Emarath_global\service_account.json'
AUTOMATED_LEAD_DATA_URL = "https://docs.google.com/spreadsheets/d/1OU786oBbozzYkvr2O9WUG3IECJfLgxVwDOt_lZRsAKo/edit?gid=661065461#gid=661065461"
SOURCE_URLS = [
    "https://docs.google.com/spreadsheets/d/1aXobnCl4QQ-hQ_IWoTPo2ZEo3HM5kNwycwMBJtEDOfc/edit",
    "https://docs.google.com/spreadsheets/d/1ztfkIIEeL9EmSdLQIGhBGIFbraMLmqEBwc8FP-CA_3c/edit"
]
TARGET_URL = "https://docs.google.com/spreadsheets/d/1OU786oBbozzYkvr2O9WUG3IECJfLgxVwDOt_lZRsAKo/edit"

BDE_NAMES = [
    'RAHIB', 'HABIYA', 'BURHANA', 'SHAMNA', 'ARUN',
    'CHAITHANYA', 'ZAKIYA', 'SAFAN',
    'ADWAITHA', 'NEHA', 'GOWTHAM', 'AMINA', 'RINSIYA', 'ARSHAD', 'NAFI',
    "SHIHAD", "RAHIYAD", 'AKASH', 'NAJIYA', 'RANJITH', "NIHAD", "AFNAN"
]

TEAM_MAP = {
    'RAHIB': 'SHAHABAS TEAM', 'HABIYA': 'SHAHABAS TEAM', 'BURHANA': 'SHAHABAS TEAM', '': 'SHAHABAS TEAM',
    'SHAMNA': 'SHAHABAS TEAM', 'ARUN': 'SHAHABAS TEAM', 'CHAITHANYA': 'SHAHABAS TEAM', 'ADWAITHA': 'SHAHABAS TEAM',
    'ZAKIYA': 'MANASA TEAM', 'SAFAN': 'MANASA TEAM', 'SHIHAD': 'MANASA TEAM',
    'ARSHAD': 'MANASA TEAM', 'NAJIYA': 'MANASA TEAM', 'NIHAD': 'MANASA TEAM',
    'GOWTHAM': 'JEFFIN TEAM', 'AMINA': 'JEFFIN TEAM', 'RINSIYA': 'JEFFIN TEAM',
    'RAHIYAD': 'JEFFIN TEAM', 'NAFI': 'JEFFIN TEAM', 'AKASH': 'JEFFIN TEAM', 'NEHA': 'JEFFIN TEAM', 
    'RANJITH': 'JEFFIN TEAM', 'AFNAN': 'JEFFIN TEAM',
}

known_agents = [
    "ADWAITHA T M", "CHAITHANYA P K", "SHIHAD", "RAHIYAD",
    "MOHAMMED RAHIB K E", "HABIYA FARHAN", "ARUN BABU", "BURHANA N R",
    "SHAMNA", "NEHA P", "AMINA NISANA V C", "GOWTHAM KRISHNA",
    "RINSY HUSSAIN", "MUHAMMED ARSHAD", "MUHAMMED SAFAN K P", "ZAKIYA GAFOOR", "AKASH", "NAJIYA", "RANJITH", "NAJIYA", "NIHAD", "AFNAN"
]

AGENT_NAME_TO_BDE = {
    "ADWAITHA T M": "ADWAITHA", "CHAITHANYA P K": "CHAITHANYA", "SHIHAD": "SHIHAD",
    "RAHIYAD": "RAHIYAD", "MOHAMMED RAHIB K E": "RAHIB", "HABIYA FARHAN": "HABIYA",
    "ARUN BABU": "ARUN", "BURHANA N R": "BURHANA", "SHAMNA": "SHAMNA",
    "NEHA P": "NEHA", "AMINA NISANA V C": "AMINA", "GOWTHAM KRISHNA": "GOWTHAM",
    "RINSY HUSSAIN": "RINSIYA", "MUHAMMED ARSHAD": "ARSHAD", "MUHAMMED SAFAN K P": "SAFAN",
    "ZAKIYA GAFOOR": "ZAKIYA", "SUSHANTHIKA": "SUHANTHIKA", "AKASH" : "AKASH",
    "NAJIYA" : "NAJIYA", "RANJITH" : "RANJITH", "NIHAD" : "NIHAD", "AFNAN" : "AFNAN"
}

POTENTIAL_TARGETS = ["SUPER HOT", "HOT", "WARM"]

def deduplicate_columns(df):
    if df.columns.duplicated().any():
        seen = {}
        new_cols = []
        for col in df.columns:
            if col in seen:
                seen[col] += 1
                new_cols.append(f"{col}.{seen[col]}")
            else:
                seen[col] = 0
                new_cols.append(col)
        df.columns = new_cols
    return df

def safe_upload(spreadsheet, df, sheet_name):
    if not df.empty:
        try:
            spreadsheet.open_sheet(sheet_name, create=True)
            needed_rows = len(df) + 10
            curr_rows = spreadsheet.sheet.row_count
            if curr_rows < needed_rows:
                spreadsheet.sheet.resize(rows=needed_rows)
            spreadsheet.sheet.batch_clear([f'A2:Z{max(curr_rows, 5000)}'])
            upload_df = df.astype(str).replace(['nan', 'None', 'NaT'], '')
            spreadsheet.df_to_sheet(upload_df, index=False, replace=False, headers=False, start="A2")
        except Exception as e:
            print(f"[!] Upload failed for {sheet_name}: {e}")

def run_update():
    print(f"\n--- Sync started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")

    try:
        c = conf.get_config(file_name=CONFIG_PATH)

        # ── 1. Load Assigned Lead Data ─────────────────────────────────────────────
        lead_spread = Spread(AUTOMATED_LEAD_DATA_URL, config=c)
        lead_df = lead_spread.sheet_to_df(sheet="LEAD ASSIGNED", index=None)

        df_filtered = lead_df[
            lead_df['Agent NAme'].str.strip().str.upper().isin([a.upper() for a in known_agents])
        ].copy()

        df_filtered['Date'] = pd.to_datetime(
            df_filtered['Date'].str.replace(r'(\d{4})(\d)', r'\1 \2', regex=True),
            format='%d/%m/%Y %I:%M %p', errors='coerce'
        )

        df_filtered['Day'] = ((df_filtered['Date'] - pd.Timedelta(hours=19)).dt.date + pd.Timedelta(days=1))
        df_filtered['Agent_BDE'] = df_filtered['Agent NAme'].str.strip().str.upper().map({k.upper(): v for k, v in AGENT_NAME_TO_BDE.items()})

        agent_daily = df_filtered.groupby(['Day', 'Agent_BDE']).size().reset_index(name='LEAD LANDED')
        agent_daily['Day'] = pd.to_datetime(agent_daily['Day'])

        # ── 2. Load Main Source Data ───────────────────────────────────────────────
        target_spread = Spread(TARGET_URL, config=c)
        all_data_list = []
        won_data_list = []

        mapping_cols = {
            'REF NO': 'REF NO', 'COUNTRY': 'REGION', 'DATE': 'DATE',
            'AGENT': 'AGENT', 'CUSTOMER PATH': 'CUSTOMER PATH', 'NAME': 'NAME',
            'PHONE NO 1': 'PHONE NO', 'STATUS': 'STATUS', 'PRODUCT 1': 'PRODUCT',
            'CALL STATUS': 'CALL STATUS', 'VALUE': 'VALUE'
        }

        for url in SOURCE_URLS:
            try:
                source_spread = Spread(url, config=c)
                existing_tabs = [s.title for s in source_spread.sheets]

                for bde in BDE_NAMES:
                    if bde not in existing_tabs: continue
                    try:
                        df = source_spread.sheet_to_df(sheet=bde, index=None, header_rows=1)
                        df = deduplicate_columns(df)

                        # Capture WON rows specifically for Value calculation
                        if 'STATUS' in df.columns:
                            won_rows = df[df['STATUS'].astype(str).str.strip().str.upper() == 'WON'].copy()
                            if not won_rows.empty:
                                for col in ['STATUS', 'AGENT']:
                                    if col in won_rows.columns:
                                        won_rows[col] = won_rows[col].astype(str).str.strip().str.upper()
                                # Convert Value for WON data specifically
                                won_rows['VALUE'] = pd.to_numeric(won_rows.get('VALUE'), errors='coerce').fillna(0)
                                won_rows['BDM_TEAM'] = won_rows['AGENT'].map(TEAM_MAP).fillna('OTHER')
                                won_rows['DATE'] = pd.to_datetime(won_rows.get('DATE'), errors='coerce', dayfirst=True)
                                won_data_list.append(won_rows)

                        # General Lead Data processing
                        if not df.empty and all(col in df.columns for col in mapping_cols.keys()):
                            subset = df[list(mapping_cols.keys())].copy()
                            subset.rename(columns=mapping_cols, inplace=True)
                            subset['DATE'] = pd.to_datetime(subset['DATE'], errors='coerce', dayfirst=True)
                            subset['CUSTOMER PATH'] = subset['CUSTOMER PATH'].astype(str).str.strip().str.upper()
                            subset = subset[subset['CUSTOMER PATH'] == 'LEAD']
                            
                            invalid_vals = ['', 'NAN', 'NONE', 'N/A', '0']
                            subset = subset[~subset['PHONE NO'].astype(str).str.strip().str.upper().isin(invalid_vals)]

                            if not subset.empty:
                                for col in ['STATUS', 'AGENT', 'CALL STATUS']:
                                    subset[col] = subset[col].astype(str).str.strip().str.upper()
                                subset['BDM_TEAM'] = subset['AGENT'].map(TEAM_MAP).fillna('OTHER')
                                all_data_list.append(subset)

                    except Exception as e: print(f"Error in {bde}: {e}")
            except Exception as e: print(f"Failed URL {url}: {e}")

        # ── 3. Final Report ──────────────────────────────────────────────────
        if all_data_list:
            master_df = pd.concat(all_data_list, ignore_index=True)
            group_cols = ['DATE', 'BDM_TEAM', 'AGENT']
            master_df['is_unattended'] = master_df['CALL STATUS'].astype(str).str.strip().isin(['', 'NAN'])

            total_leads = master_df.groupby(group_cols)['PHONE NO'].count().to_frame('TOTAL LEADS ASSIGNED')
            status_pivot = master_df.pivot_table(index=group_cols, columns='STATUS', aggfunc='size', fill_value=0).add_suffix('_S')
            
            unattended_pivot = master_df[master_df['is_unattended']].groupby(group_cols).size().reindex(total_leads.index, fill_value=0).to_frame('UNATTENDED')

            # ── WON VALUE AND BREAKDOWN ───────────────────────────────────────
            won_value_pivot = None
            won_status_pivot = None

            if won_data_list:
                won_df = pd.concat(won_data_list, ignore_index=True)
                # Calculate Sum of WON Value
                won_value_pivot = won_df.groupby(group_cols)['VALUE'].sum().to_frame('TOTAL WON VALUE')
                
                # Calculate Status breakdown for WON
                won_status_pivot = won_df.pivot_table(index=group_cols, columns='STATUS', aggfunc='size', fill_value=0)
                won_status_pivot.columns = [f"TOTAL - {col}" for col in won_status_pivot.columns]

            # ── JOINING ───────────────────────────────────────────────────────
            final_report = total_leads.join([status_pivot, unattended_pivot], how='left')
            if won_status_pivot is not None:
                final_report = final_report.join(won_status_pivot, how='left')
            if won_value_pivot is not None:
                final_report = final_report.join(won_value_pivot, how='left')

            final_report = final_report.fillna(0)
            final_report.columns = [col.replace('_S', '') if col.endswith('_S') else col for col in final_report.columns]

            # Potential calculation
            present_potentials = [c for c in POTENTIAL_TARGETS if c in final_report.columns]
            final_report['POTENTIAL LEAD'] = final_report[present_potentials].sum(axis=1) if present_potentials else 0

            final_report = final_report.reset_index()
            final_report['DATE'] = pd.to_datetime(final_report['DATE'])
            
            # Merge with Lead Landed data
            final_report['_merge_date'] = final_report['DATE'].dt.normalize()
            agent_daily_merge = agent_daily.rename(columns={'Day': '_merge_date', 'Agent_BDE': 'AGENT', 'LEAD LANDED': '_LL'})
            final_report = final_report.merge(agent_daily_merge[['_merge_date', 'AGENT', '_LL']], on=['_merge_date', 'AGENT'], how='left')
            final_report['LEAD LANDED'] = final_report['_LL'].fillna(0).astype(int)
            final_report.drop(columns=['_merge_date', '_LL'], inplace=True)

            # ── COLUMN ORDERING ────────────────────────────────────────────────
            won_breakdown_cols = [c for c in final_report.columns if c.startswith('TOTAL -')]
            ordered_statuses = [s for s in ["WON", "SUPER HOT", "HOT", "WARM", "COLD", "BOOKING", "WHATS APP ENGAGE", "UNATTENDED"] if s in final_report.columns]
            
            front_cols = ['DATE', 'BDM_TEAM', 'AGENT', 'LEAD LANDED', 'TOTAL LEADS ASSIGNED', 'POTENTIAL LEAD']
            
            # Put TOTAL WON VALUE at the absolute end
            final_cols = front_cols + won_breakdown_cols + ordered_statuses + ['TOTAL WON VALUE']
            
            final_report = final_report.reindex(columns=final_cols).sort_values(by=['DATE', 'BDM_TEAM', 'AGENT'])
            
            safe_upload(target_spread, final_report, "DATA")
            print(f"[✓] Success: {len(final_report)} rows synced.")

    except Exception as e: print(f"CRITICAL ERROR: {e}")

# --- EXECUTION LOOP ---
if __name__ == "__main__":
    print("--- BDM Dashboard Automation Started ---")
    try:
        while True:
            run_update()
            print("Waiting for 15 minutes...")
            time.sleep(900)
    except KeyboardInterrupt:
        print(f"Last update attempted at: {datetime.now().strftime('%H:%M:%S')}")
        print("Automation stopped. Goodbye!")
        sys.exit(0)