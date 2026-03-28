import pandas as pd
import numpy as np
from datetime import datetime
from gspread_pandas import Spread, conf
import time
import sys
import re

# --- CONFIGURATION ---
CONFIG_PATH = r'C:\Users\USER\Downloads\Emarath_global\service_account.json'
TARGET_SHEET_URL = "https://docs.google.com/spreadsheets/d/1X5ayTQNLratOzsFNiCEkvegDJ645gaMWoNbz1fEThA0/edit"

# ADD NEW MONTHLY URLs HERE
LOGISTIC_SOURCES = [
    # "https://docs.google.com/spreadsheets/d/1S6WaI1rXuf5ogN44ph8weA_z_bMCg85U3uaZ7RHvpfM/edit", # Feb
    "https://docs.google.com/spreadsheets/d/1S6WaI1rXuf5ogN44ph8weA_z_bMCg85U3uaZ7RHvpfM/edit", # Mar
]

INTERVAL_MINUTES = 15

MANUAL_VENDOR_MAP = {
    'OUD LOVERS': 'LPG', 
    'INTENSE SIGNATURE': 'LPG', 
    'ARBE PURO COMBO': 'LPG',
    'CHERIE BLOSSOM': 'LPG', 
    'VELORA POP HEART': 'LPG', 
    'VELORA SUGAR BLISS': 'LPG',
    'VELORA VIVA CHOCO': 'LPG', 
    'ASTORIA': 'LPG', 
    'JENAN': 'LPG',
    'NAJAH PISTACHIO': 'LPG', 
    'LEON': 'LPG', 
    'OPUS': 'LPG', 
    'ENIGMA': 'LPG',
    'RANIA': 'LPG', 

    'COLLECTION OF MOOD' : 'ATYAF KSA',
    'ASEEL COMBO' : 'ATYAF KSA',
    'HECTOR COMBO' : 'ATYAF KSA',
    'MIRAMAR COMBO' : 'ATYAF KSA',
    'SHADOW FLAME' : 'ATYAF KSA',
    'VOLGA COMBO' : 'ATYAF KSA',
    'ARCHER COMBO': 'ATYAF',

    'PREMIUM EDITION': 'OUD AL SALAM', 
    'ABSOLUTE MOUNTAIN AVENUE': 'OUD AL SALAM', 
    'ABSOLUTE MOUNTAIN AVENUE 3 PCS- SAUDIARABIA': 'OUD AL SALAM', 
    'JOYOUS PREMIUM EDITION 5PCS- SAUDIARABIA': 'OUD AL SALAM', 
    'ABSOLUTE MOUNTAIN AVENUE': 'OUD AL SALAM', 

    'SEVEN DAYS': 'RT',
    'OLD MEMMORIES': 'RT', 
    '7 PS COMBO': 'RT',

    'CLIVE COLLECTION' : 'SCENT PASSION',
    'DOE COLLECTION' : 'SCENT PASSION',
    'ESENCIA FLORAL' : 'SCENT PASSION',
    'AMEERATH UL ARAB' : 'SCENT PASSION',
    'AMBER' : 'SCENT PASSION',
    'FERRAGAMO' : 'SCENT PASSION',
    'SUFI COMBO' : 'SCENT PASSION',
    # 'CLIVE COLLECTION' : 'SCENT PASSION',
}

# Standardizing Column Names
REQUIRED_COLUMNS = [
    'COUNTRY', 'AGENT', 'DATE', 'TRACKING NUMBER', 'EM NUMBER',
    'NAME', 'NUMBER1', 'NUMBER2', 'STATE / CITY', 'ADDRESS',
    'CUSTOMER PATH', 'STATUS', 'DISPATCHED DATE', 'REASON', 
    'DELIVERY AGENT', 'PRODUCT1', 'QTY', 'PRODUCT2', 'QTY2', 'TOTAL', 
]

COLUMN_RENAME_MAP = {
    'TRACKING \nNUMBER': 'TRACKING NUMBER',
    'EMNUMBER': 'EM NUMBER',
    'CUSTOMER\nPATH': 'CUSTOMER PATH',
    'DISPATCHED\nDATE': 'DISPATCHED DATE',
    'NATIONAL \nCODE': 'NATIONAL CODE',
    # 'DELIVERY \nAGENT \n(AVAILABLE)' : 'DELIVERY AGENT',
}

def clean_and_sum_digits(value):
    val_str = str(value).strip().lower()
    if not val_str or val_str == 'nan': return 0
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
        df = df[df['AGENT'].astype(str).str.strip().ne('') & df['EM NUMBER'].astype(str).str.strip().ne('') & df['PRODUCT1'].astype(str).str.strip().ne('')]
        return df
    except Exception:
        return pd.DataFrame()

def process_and_sync():
    try:
        now_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{now_ts}] Starting multi-month sync...")
        
        c = conf.get_config(file_name=CONFIG_PATH)
        target_spread = Spread(TARGET_SHEET_URL, config=c)
        
        all_dataframes = []

        # --- LOOP THROUGH ALL MONTHLY SHEETS ---
        for url in LOGISTIC_SOURCES:
            spread = Spread(url, config=c)
            ksa = load_and_standardize(spread, "ORDER LIST - KSA", "KSA")
            uae = load_and_standardize(spread, "ORDER LIST - UAE", "UAE")
            qatar = load_and_standardize(spread, "ORDER LIST - QATAR", "QATAR")
            bahrain = load_and_standardize(spread, "ORDER LIST - BAHRAIN", "BAHRAIN")
            all_dataframes.extend([ksa, uae, qatar, bahrain])

        df = pd.concat(all_dataframes, ignore_index=True)
        # df.to_excel("./df_sheet.xlsx")
        if df.empty:
            print("No data found across all sheets.")
            return

        # --- DATA CLEANING ---
        df['DATE'] = pd.to_datetime(df['DATE'], dayfirst=True, errors='coerce').dt.date
        df = df.dropna(subset=['DATE'])
        
        cat_cols = ['STATUS', 'DELIVERY AGENT', 'COUNTRY', 'PRODUCT1']
        for col in cat_cols:
            df[col] = df[col].astype(str).str.strip().str.upper()

        df['VENDOR'] = df['PRODUCT1'].map(MANUAL_VENDOR_MAP).fillna('UNKNOWN VENDOR')
        df['TOTAL'] = df['TOTAL'].apply(clean_and_sum_digits)

        # --- REPORT 1: LOGISTICS SUMMARY ---
        report = df.groupby('DATE').size().reset_index(name='TOTAL WON ORDERS')
        
        for country in ['KSA', 'UAE', 'QATAR', 'BAHRAIN']:
            c_data = df[df['COUNTRY'] == country].groupby('DATE').size()
            report = report.merge(c_data.rename(f'{country} ORDERS'), on='DATE', how='left')

        agents = ['TAWSEEL', 'NAQEL', 'RGS', 'FETCH SAUDI', 'FETCH QATAR', 'FETCH BAHRAIN', 'JNT']
        for agent in agents:
            a_data = df[df['DELIVERY AGENT'] == agent].groupby('DATE').size()
            report = report.merge(a_data.rename(agent), on='DATE', how='left')

        combos = {
            'KSA-RGS': ('RGS', 'KSA'),
            'KSA-NAQEL': ('NAQEL', 'KSA'),
            'KSA-FETCH_SAUDI': ('FETCH SAUDI', 'KSA'),
            'UAE-TAWSEEL': ('TAWSEEL', 'UAE'),
            'QATAR-FETCH_QATAR': ('FETCH QATAR', 'QATAR'),
            'BAHRAIN-FETCH_BAHRAIN': ('FETCH BAHRAIN', 'BAHRAIN')
        }
        for col_name, (agent, country) in combos.items():
            report[col_name] = df[(df['DELIVERY AGENT'] == agent) & (df['COUNTRY'] == country)].groupby('DATE').size()

        statuses = ['DELIVERED AND UNPAID', 'OUT FOR DELIVERY', 'RTO', 'ON HOLD', 'NO ANSWER', 'CANCELLED', 'ORDER CONFIRMED', 'DISPATCHED']
        for status in statuses:
            s_data = df[df['STATUS'] == status].groupby('DATE').size()
            report = report.merge(s_data.rename(status), on='DATE', how='left')

        report = report.fillna(0).sort_values('DATE', ascending=True)

        # --- REPORT 2: PRODUCT & FINANCIAL ---
        merge_keys = ['COUNTRY', 'DELIVERY AGENT', 'VENDOR', 'PRODUCT1']
        detailed = df.groupby(merge_keys)['TOTAL'].agg(['size', 'sum']).reset_index()
        detailed.columns = merge_keys + ['TOTAL_ORDERS', 'TOTAL_REVENUE']

        # Status specific sums
        for status_name, prefix in [('DELIVERED AND UNPAID', 'DELIVERED'), ('RTO', 'RTO'), ('CANCELLED', 'CANCELLED')]:
            temp = df[df['STATUS'] == status_name].groupby(merge_keys)['TOTAL'].agg(['size', 'sum']).reset_index()
            temp.columns = merge_keys + [f'{prefix}_ORDERS', f'{prefix}_REVENUE']
            detailed = detailed.merge(temp, on=merge_keys, how='left')

        detailed = detailed.fillna(0)

        # Daily Summary by Country/Vendor
        df['QTY'] = pd.to_numeric(df['QTY'], errors='coerce').fillna(0)
        df['QTY2'] = pd.to_numeric(df['QTY2'], errors='coerce').fillna(0)

        p1 = df[['DATE', 'COUNTRY', 'VENDOR', 'STATUS', 'DELIVERY AGENT', 'PRODUCT1', 'QTY']].copy()
        p1.rename(columns={'PRODUCT1': 'PRODUCT', 'QTY': 'QTY'}, inplace=True)

        p2 = df[['DATE', 'COUNTRY', 'VENDOR', 'STATUS', 'DELIVERY AGENT', 'PRODUCT2', 'QTY2']].copy()
        p2.rename(columns={'PRODUCT2': 'PRODUCT', 'QTY2': 'QTY'}, inplace=True)

        combined_products = pd.concat([p1, p2], ignore_index=True)

        # 5. Remove rows where Product is empty or Qty is 0
        combined_products = combined_products[
            (combined_products['PRODUCT'].astype(str).str.strip() != "") & 
            (combined_products['QTY'] > 0)
        ]

        # 6. Final GroupBy (Summing all quantities)
        report_by_country = (
            combined_products.groupby(['DATE', 'COUNTRY', 'VENDOR', 'DELIVERY AGENT', 'PRODUCT'])['QTY']
            .sum()
            .reset_index(name='SALES_COUNT')
        )

        # report_by_country = df.groupby(['DATE', 'COUNTRY', 'VENDOR', 'DELIVERY AGENT', 'PRODUCT1']).size().reset_index(name='SALES_COUNT')

        # Daily Summary by Country and product sold
        opening_date = datetime.strptime("04/03/2026", "%d/%m/%Y").date()
        stock_open_df = combined_products[combined_products['DATE'] >= opening_date]
        report_by_country_product_sold = stock_open_df.groupby(['COUNTRY', 'STATUS', 'PRODUCT'])['QTY'].sum().reset_index(name='SALES_COUNT')

        # --- UPLOAD ---
        safe_upload(target_spread, report, "Logistic_Summary_Report")
        safe_upload(target_spread, detailed, "Product_Consolidated_Report")
        safe_upload(target_spread, report_by_country, "Product_Summary_Report")
        safe_upload(target_spread, report_by_country_product_sold, "Product_Sold_Report")

        print(f"[{datetime.now().strftime('%H:%M:%S')}] Sync Successful.")

    except Exception as e:
        print(f"Error: {str(e)}")



if __name__ == "__main__":
    print("Scheduler started.")
    try:
        while True:
            process_and_sync()
            time.sleep(INTERVAL_MINUTES * 60)
    except KeyboardInterrupt:
        print("\nShutdown signal received.")
        sys.exit(0)