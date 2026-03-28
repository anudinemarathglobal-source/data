import pandas as pd
import re
import numpy as np
import time
import schedule
import sys
from datetime import datetime
from gspread_pandas import Spread, conf

# --- CONFIGURATION ---
CONFIG_PATH = r'C:\Users\USER\Downloads\Emarath_global\service_account.json'
SOURCE_SHEET_URL = "https://docs.google.com/spreadsheets/d/1ztfkIIEeL9EmSdLQIGhBGIFbraMLmqEBwc8FP-CA_3c/edit?gid=822921427"
LOGISTIC_SHEET_URL = "https://docs.google.com/spreadsheets/d/1eo2tY_57lcOTtGOyU5GOz2IQON3b0VsVf8rTsbfT3HM/edit?gid=0#gid=0"
TARGET_SHEET_URL = "https://docs.google.com/spreadsheets/d/1ndt_6iIRLEihA8OOA8GyaS0GkpuYTSU3ZZfZT75JkkU/edit?gid=1468096196#gid=1468096196"

# --- HELPER FUNCTIONS ---
def is_uae_number(phone):
    if pd.isna(phone): return False
    clean_phone = re.sub(r'\D', '', str(phone))
    return bool(re.match(r'^(?:971|0)?5[024568]\d{7}$', clean_phone))

def get_clean_local_number(phone, pattern):
    if pd.isna(phone): return None
    clean_phone = re.sub(r'\D', '', str(phone))
    match = re.match(pattern, clean_phone)
    return match.group(1) if match else None

def safe_upload(spreadsheet, df, sheet_name):
    if not df.empty:
        try:
            spreadsheet.open_sheet(sheet_name, create=True)
            spreadsheet.sheet.batch_clear(['A2:Z10000']) 
            upload_df = df.astype(str)
            spreadsheet.df_to_sheet(upload_df, index=False, replace=False, headers=False, start="A2")
            print(f"  [SUCCESS] Uploaded {len(df)} rows to {sheet_name}")
        except Exception as e:
            print(f"  [ERROR] Failed to upload to {sheet_name}: {e}")


# --- PROCESSING LOGIC ---
def run_sync():
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n--- Starting Sync Task: {timestamp} ---")
    
    try:
        # Initialize Connections
        c = conf.get_config(file_name=CONFIG_PATH)
        spread = Spread(LOGISTIC_SHEET_URL, config=c)
        target_spread = Spread(TARGET_SHEET_URL, config=c)

        # Load and Clean Data
        df = spread.sheet_to_df(sheet="Order list", index=None)
        df.columns = df.columns.astype(str).str.strip().str.upper()
        df = df[df['STATUS'].astype(str).str.strip().isin(["", "nan", "None", "NULL"])]

        if 'DATE' not in df.columns:
            print("  [SKIP] 'DATE' column missing.")
            return

        df['DATE'] = pd.to_datetime(df['DATE'], dayfirst=True, errors='coerce').dt.date
        df = df.dropna(subset=['DATE'])

        if 'TRACKING NUMBER' in df.columns:
            df = df[df['TRACKING NUMBER'].astype(str).str.strip() == ""]

        # Process UAE
        uae_pattern = r'^(?:971|0)?(5\d{8})$'
        uae_raw = df[df['COUNTRY'] == 'UAE'].copy()
        uae_raw['STATUS'] = np.nan
        uae_raw['CLEAN_NO'] = uae_raw['NUMBER1'].apply(lambda x: get_clean_local_number(x, uae_pattern))
        uae_filtered = uae_raw[uae_raw['CLEAN_NO'].notna()].copy()
        # uae_filtered = uae_raw[uae_raw['NUMBER1'].apply(is_uae_number)].copy()
        
        formatted_uae = pd.DataFrame({
            'CUSTOMER_NAME': uae_filtered['NAME'], 
            'MOBILE_NO': uae_filtered['NUMBER1'],
            'LANDLINE_NO': uae_filtered['NUMBER2'], 
            'ADDRESS_1': uae_filtered['ADDRESS'],
            'ADDRESS_2': "", 
            'ADDRESS_3': "", 
            'FLAT/VILLANO': "",
            'DELIVERY_CITY': uae_filtered['STATE/CITY'], 
            'COD_AMOUNT': uae_filtered['TOTAL'],
            'REMARKS': uae_filtered['PRODUCT1'], 
            'REFERENCE_NO': uae_filtered['EM NUMBER'],
            'OTHER_REMARKS': uae_filtered['NOTES']
        })

        
        def map_fetch_df(source_df, country_name):
            source_id = f"SCENT PASSION - {country_name}"
            price_list = "Default QAR pricelist" if country_name == "QATAR" else "Default BHD pricelist"
            
            return pd.DataFrame({
                "client_order_ref": source_df['EM NUMBER'], 
                "customer_name": source_df['NAME'],
                "partner_id": source_df['CLEAN_NO'], 
                "whatsapp_no": source_df['CLEAN_NO'],
                "source_id": source_id, 
                "Pricelist Name": price_list,
                "street_no": source_df['ADDRESS'], 
                "building_no": "", 
                "zone_id (governarate)": source_df['ZONE'],
                "wilayat_id": source_df['WILAYAT'], 
                "city_id": source_df['STATE/CITY'],
                "order_line/product_id": source_df['PRODUCT1'], 
                "order_line/product_uom": "Unit",
                "order_line/price_unit": source_df['TOTAL'], 
                "order_line/product_uom_qty remarks": source_df['REMARKS']
            })

        # Process Qatar
        qatar_raw = df[df['COUNTRY'] == 'QATAR'].copy()
        uae_raw['STATUS'] = np.nan
        qatar_pattern = r'^(?:974|0)?([3567]\d{7})$'
        qatar_raw['CLEAN_NO'] = qatar_raw['NUMBER1'].apply(lambda x: get_clean_local_number(x, qatar_pattern))
        qatar_filtered = qatar_raw[qatar_raw['CLEAN_NO'].notna()].copy()
        formatted_qatar = map_fetch_df(qatar_filtered, "QATAR")

        # Process Bahrain
        bahrain_raw = df[df['COUNTRY'] == 'BAHRAIN'].copy()
        bahrain_raw['STATUS'] = np.nan
        bahrain_pattern = r'^(?:973|0)?([136]\d{7})$'
        bahrain_raw['CLEAN_NO'] = bahrain_raw['NUMBER1'].apply(lambda x: get_clean_local_number(x, bahrain_pattern))
        bahrain_filtered = bahrain_raw[bahrain_raw['CLEAN_NO'].notna()].copy()
        formatted_bahrain = map_fetch_df(bahrain_filtered, "BAHRAIN")

        # Final Uploads
        safe_upload(target_spread, formatted_uae, 'UAE_TAWSEEL')
        safe_upload(target_spread, formatted_qatar, 'FETCH_QATAR')
        safe_upload(target_spread, formatted_bahrain, 'FETCH_BAHRAIN')

        print(f"--- Sync Completed Successfully at {datetime.now().strftime('%H:%M:%S')} ---")

    except Exception as e:
        print(f"  [CRITICAL ERROR] {e}")



# --- SCHEDULER ---

if __name__ == "__main__":
    print("Logistic Analysis Sync Service Started...")
    print("Running every 5 minutes. Press Ctrl+C to exit.")
    
    run_sync() 
    schedule.every(5).minutes.do(run_sync) 

    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[EXIT] Service stopped by user. Goodbye!")
        sys.exit(0)