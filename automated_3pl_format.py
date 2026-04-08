import pandas as pd
import re
import time
import os
from datetime import datetime
from difflib import get_close_matches
from gspread_pandas import Spread, conf
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)


CONFIG_DIR  = r'C:\Users\USER\Downloads\Emarath_global'
CONFIG_FILE = 'service_account.json'

LOGISTIC_SOURCES = [
    "https://docs.google.com/spreadsheets/d/1S6WaI1rXuf5ogN44ph8weA_z_bMCg85U3uaZ7RHvpfM/edit"
]
TARGET_SHEET_URL = "https://docs.google.com/spreadsheets/d/1ndt_6iIRLEihA8OOA8GyaS0GkpuYTSU3ZZfZT75JkkU/edit"

PATTERNS = {
    'KSA':     r'^(?:966|00966|0)?(5\d{8})$',
    'UAE':     r'^(?:971|00971|0)?(5\d{8})$',
    'QATAR':   r'^(?:974|00974|0)?([3567]\d{7})$',
    'BAHRAIN': r'^(?:973|00973|0)?([136]\d{7})$'
}

REQUIRED_COLUMNS = [
    'COUNTRY', 'AGENT', 'DATE', 'TRACKING NUMBER', 'EMNUMBER', 'NAME',
    'NUMBER1', 'NUMBER2', 'STATE / CITY', 'ADDRESS', 'STATUS',
    'DELIVERY AGENT', 'PRODUCT1', 'PRODUCT2', 'TOTAL', 'REMARKS',
    'ZONE', 'WILAYAT', 'NATIONAL CODE'
]

MAPPINGS = {
    "RGS_KSA": {
        "Shipment Reference": "EMNUMBER", "Customer Country": "COUNTRY", "Customer Name": "NAME",
        "Contact Number": "CLEAN_NO", "Complete Address": "ADDRESS", "Customer City": "STATE / CITY",
        "COD Amount": "TOTAL", "Email": "", "Description/Content": "PRODUCT1", "Branded": "NO",
        "Location Cordinates": "", "Whatsapp Number": "NUMBER2", "Insurance": "NO",
        "Deliver Service": "DELIVERY AGENT", "Warehouse Location": "",
        "National Address": "NATIONAL CODE", "Remarks": "REMARKS"
    },
    "NAQEL_KSA": {
        "Shipment Reference": "EMNUMBER", "Customer Country": "COUNTRY", "Customer Name": "NAME",
        "Contact Number": "CLEAN_NO", "Complete Address": "ADDRESS", "Customer City": "STATE / CITY",
        "COD Amount": "TOTAL", "Email": "", "Description/Content": "PRODUCT1", "Branded": "NO",
        "Location Cordinates": "", "Whatsapp Number": "NUMBER2", "Insurance": "NO",
        "Deliver Service": "DELIVERY AGENT", "Warehouse Location": "",
        "National Address": "NATIONAL CODE", "Remarks": "REMARKS"
    },
    "TAWSEEL_GENERAL": {
        "CUSTOMER_NAME": "NAME", "MOBILE_NO": "CLEAN_NO", "LANDLINE_NO": "NUMBER2",
        "ADDRESS_1": "ADDRESS", "ADDRESS_2": "", "ADDRESS_3": "", "FLAT/VILLA_NO": "",
        "DELIVERY_CITY": "STATE / CITY", "COD_AMOUNT": "TOTAL",
        "PRODUCT": "PRODUCT1", "QTY": "Unit",
        "REFERENCE_NO": "EMNUMBER", "OTHER_REMARKS": "REMARKS"
    },
    "FETCH_GENERAL": {
        "client_order_ref": "EMNUMBER", "customer_name": "NAME", "partner_id": "CLEAN_NO",
        "whatsapp_no": "NUMBER2", "source_id": "SOURCE_VAL", "Pricelist Name": "PRICE_VAL",
        "street_no": "ADDRESS", "building_no": "", "zone_id (governarate)": "ZONE",
        "wilayat_id": "WILAYAT", "city_id": "STATE / CITY", "order_line/product_id": "PRODUCT1",
        "order_line/product_uom": "Unit", "order_line/price_unit": "TOTAL",
        "order_line/product_uom_qty remarks": "REMARKS"
    }
}


# --- HELPER FUNCTIONS ---

def is_indian_number(val):
    if pd.isna(val) or str(val).strip() in ("", "nan", "None"):
        return False
    clean = re.sub(r'\D', '', str(val))
    return clean.startswith('91') and len(clean) >= 11

def clean_phone_logic(row):
    country = str(row.get('COUNTRY', '')).strip().upper()
    pattern = PATTERNS.get(country, r'(\d+)')

    def get_local_part(val):
        if pd.isna(val) or str(val).strip() in ("", "nan", "None"):
            return None
        clean = re.sub(r'\D', '', str(val))
        if not clean:
            return None
        match = re.match(pattern, clean)
        return match.group(1) if (match and match.lastindex and match.lastindex >= 1) else clean

    num1 = row.get('NUMBER1')
    num2 = row.get('NUMBER2')

    if is_indian_number(num1):
        num1, num2 = num2, num1  

    local_num = get_local_part(num1)
    if not local_num:
        local_num = get_local_part(num2)

    fallback = str(num2 if num2 else '')
    fallback = '' if str(fallback).lower() in ('nan', 'none', '') else str(fallback).strip()

    return local_num if local_num else fallback

# def is_indian_number(val):
#     if pd.isna(val) or str(val).strip() in ("", "nan", "None"):
#         return False
#     clean = re.sub(r'\D', '', str(val))
#     # Indian numbers: start with 91 and are 12 digits, or start with +91
#     return clean.startswith('91') and len(clean) >= 11

# def is_local_number(val, country='UAE'):
#     if pd.isna(val) or str(val).strip() in ("", "nan", "None"):
#         return False
#     clean = re.sub(r'\D', '', str(val))
#     return bool(clean) and not (clean.startswith('91') and len(clean) >= 11)

# def clean_phone_logic(row):
#     country = str(row.get('COUNTRY', '')).strip().upper()
#     pattern = PATTERNS.get(country, r'(\d+)')

#     def get_local_part(val):
#         if pd.isna(val) or str(val).strip() in ("", "nan", "None"):
#             return None
#         clean = re.sub(r'\D', '', str(val))
#         if not clean:
#             return None
#         match = re.match(pattern, clean)
#         return match.group(1) if (match and match.lastindex and match.lastindex >= 1) else clean

#     num1 = row.get('NUMBER1')
#     num2 = row.get('NUMBER2')

#     num1_is_indian = is_indian_number(num1)
#     num2_is_local = is_local_number(num2)

#     # ✅ KEY LOGIC: If NUMBER1 is Indian and NUMBER2 is local → use NUMBER2
#     if num1_is_indian and num2_is_local:
#         primary = num2
#     else:
#         # Use NUMBER1 if valid, else fall back to NUMBER2
#         num1_clean = re.sub(r'\D', '', str(num1)) if not pd.isna(num1) else ''
#         primary = num1 if num1_clean else num2

#     local_num = get_local_part(primary)

#     # Final fallback
#     if not local_num:
#         fallback = str(num2 if num1_is_indian else num1)
#         fallback = '' if fallback.lower() in ('nan', 'none', '') else fallback.strip()
#         local_num = get_local_part(fallback) or fallback

#     return local_num if local_num else ''



def split_products(df, mapping_dict, source_id=None, price_list=None):
    if df is None or df.empty:
        return pd.DataFrame()

    df = df[df['EMNUMBER'].notna() & (df['EMNUMBER'].astype(str).str.strip() != '')].copy()
    if df.empty:
        return pd.DataFrame()

    # Ensure PRODUCT2 column exists before accessing it
    if 'PRODUCT2' not in df.columns:
        df['PRODUCT2'] = ''

    df = df.assign(_sort=range(len(df)))

    # --- P1 rows ---
    p1 = pd.DataFrame(index=df.index)
    for target_col, source_col in mapping_dict.items():
        if source_col in df.columns:
            p1[target_col] = df[source_col].values
        else:
            p1[target_col] = source_col  

   
    if source_id is not None:
        p1['source_id']      = source_id
    if price_list is not None:
        p1['Pricelist Name'] = price_list
    p1['_sort'] = df['_sort'].values
    p1['_pos']  = 1

    # --- P2 rows ---
    has_p2 = df[df['PRODUCT2'].fillna('').astype(str).str.strip().ne('')].copy()
    if has_p2.empty:
        return p1.drop(columns=['_sort', '_pos']).reset_index(drop=True)

    p2 = pd.DataFrame(index=has_p2.index)
    for target_col, source_col in mapping_dict.items():
        if source_col == "PRODUCT1":
            p2[target_col] = has_p2['PRODUCT2'].values
        elif source_col in ("REMARKS", "TOTAL"):
            p2[target_col] = has_p2[source_col].values if source_col in has_p2.columns else ""
        elif source_col == "EMNUMBER":
            p2[target_col] = ""
        else:
            p2[target_col] = ""

    p2['source_id']      = "" if source_id   is not None else pd.NA
    p2['Pricelist Name'] = "" if price_list  is not None else pd.NA

    # Drop any all-NA columns introduced above (keeps Tawseel output clean)
    p2 = p2.dropna(axis=1, how='all')
    p2['_sort']          = has_p2['_sort'].values
    p2['_pos']           = 2

    combined = pd.concat([p1, p2]).sort_values(['_sort', '_pos'])
    return combined.drop(columns=['_sort', '_pos']).reset_index(drop=True)



def safe_upload(spreadsheet, df, sheet_name):
    if not df.empty:
        try:
            spreadsheet.open_sheet(sheet_name, create=True)
            spreadsheet.sheet.batch_clear(['A2:Z2000']) 
            spreadsheet.df_to_sheet(df, index=False, replace=False, headers=False, start="A2")
            print(f"Successfully updated sheet: {sheet_name}")
        except Exception as e:
            print(f"Error uploading to {sheet_name}: {e}")


# --- MAIN SYNC TASK ---

def run_sync():
    print(f"\n--- Sync Starting: {datetime.now().strftime('%H:%M:%S')} ---")
    try:
     
        c = conf.get_config(conf_dir=CONFIG_DIR, file_name=CONFIG_FILE)
        target_spread = Spread(TARGET_SHEET_URL, config=c)

        all_frames = []
        for url in LOGISTIC_SOURCES:
            s = Spread(url, config=c)
            for sheet, country in [
                ("ORDER LIST - KSA",     "KSA"),
                ("ORDER LIST - UAE",     "UAE"),
                ("ORDER LIST - QATAR",   "QATAR"),
                ("ORDER LIST - BAHRAIN", "BAHRAIN"),
            ]:
                try:
                    df = s.sheet_to_df(sheet=sheet, index=None)
                    df.columns = df.columns.astype(str).str.strip().str.upper()
                    df['COUNTRY'] = country
                    all_frames.append(df)
                    print(f"  [✓] Loaded {sheet} ({len(df)} rows)")
                except Exception as e:
                    print(f"  [~] Skipped {sheet}: {e}")
                    continue

        if not all_frames:
            print("  No data loaded from any source sheet.")
            return

        full_df = pd.concat(all_frames, ignore_index=True)
        for col in REQUIRED_COLUMNS:
            if col not in full_df.columns:
                full_df[col] = ""

        # Filter: no tracking number, and status is blank or ORDER CONFIRMED
        untracked = full_df[
            (full_df['TRACKING NUMBER'].astype(str).str.strip() == "") &
            (
                (full_df['STATUS'].astype(str).str.strip() == "") |
                (full_df['STATUS'].astype(str).str.strip().str.upper() == "ORDER CONFIRMED")
            )
        ].copy()

        indian_mask = untracked['NUMBER1'].apply(is_indian_number)

        untracked.loc[indian_mask, ['NUMBER1', 'NUMBER2']] = (
            untracked.loc[indian_mask, ['NUMBER2', 'NUMBER1']].values
        )

        untracked['CLEAN_NO'] = untracked.apply(clean_phone_logic, axis=1)

        # untracked['CLEAN_NO'] = untracked.apply(clean_phone_logic, axis=1)

        # --- KSA splits ---
        ksa       = untracked[untracked['COUNTRY'] == 'KSA']
        rgs_ksa   = split_products(ksa[ksa['DELIVERY AGENT'] == 'RGS'],         MAPPINGS["RGS_KSA"])
        naqel_ksa = split_products(ksa[ksa['DELIVERY AGENT'] == 'NAQEL KSA'],   MAPPINGS["NAQEL_KSA"])
        fetch_ksa = split_products(ksa[ksa['DELIVERY AGENT'] == 'FETCH SAUDI'], MAPPINGS["FETCH_GENERAL"],
                                   "SCENT PASSION - SAUDIARABIA", "Default SAR pricelist")

        # --- Other countries ---
        qatar   = split_products(untracked[untracked['COUNTRY'] == 'QATAR'],   MAPPINGS["FETCH_GENERAL"],
                                 "SCENT PASSION - QATAR",   "Default QAR pricelist")
        bahrain = split_products(untracked[untracked['COUNTRY'] == 'BAHRAIN'], MAPPINGS["FETCH_GENERAL"],
                                 "SCENT PASSION - BAHRAIN", "Default BHD pricelist")

        # --- UAE splits by product ---
        def fuzzy_match_product(product_val, product_list, cutoff=0.82):
            val = str(product_val).strip().upper()
            if not val:
                return False
            if val in [p.upper() for p in product_list]:
                return True
            matches = get_close_matches(val, [p.upper() for p in product_list], n=1, cutoff=cutoff)
            if matches:
                print(f"    [~] Fuzzy matched '{val}' → '{matches[0]}'")
                return True
            return False

        def get_uae_data(df, product_list, source_id=None, price_list=None):
          
            mask = (
                (df['COUNTRY'] == 'UAE') &
                (df['PRODUCT1'].fillna('').astype(str)
                   .apply(lambda x: fuzzy_match_product(x, product_list)))
            )
            filtered_df = df[mask].copy()

            if filtered_df.empty:
                return pd.DataFrame()

            return split_products(filtered_df, MAPPINGS["TAWSEEL_GENERAL"], source_id, price_list)

        sant_tawseel  = get_uae_data(untracked, [
            'DOE COLECTION', 'ESCENTIAL FLORAL', 'PEACOCK COLLECTION', 'CLIVE COLLECTION',
            'AMEERATH UL ARAB', 'FERRAGAMO', 'SUFI', 'MARYAM COMBO', 'MUSK COLLECTION',
            'FERRAGAMO & SUFI COMBO', 'SALTY FLOWER', 'AMBER', 'AMBER+GIFT',
            'SALTY FLOWER+GIFT', 'LORACHE', 'MOJEH'
        ])
        oud_tawseel   = get_uae_data(untracked, ['PREMIUM EDITION', 'ABSOLUTE MOUNTAIN AVENUE', 'AL HUDA'])
        lpg_tawseel   = get_uae_data(untracked, [
            'EXCLUSIF COMBO', 'OUD LOVERS', 'INTENSE SIGNATURE',
            'LA FLORAL', 'CHERRY BLOSSOM', 'ARBEPURO COMBO', 'INTENSE PINK', 'INTENSE BLACK', 'INTENSE BROWNE'
        ])
        rt_tawseel    = get_uae_data(untracked, ['SEVEN DAYS', 'OLD MEMORIES'])
        bsparq_tawseel = get_uae_data(untracked, ['MAQAM IBRAHIM-BSPARQ', 'MUKHALAT EMARATI'])

        # --- Upload all ---
        safe_upload(target_spread, rgs_ksa, 'RGS_KSA_TEMPLATE')
        safe_upload(target_spread, naqel_ksa, 'NAQEL_KSA_TEMPLATE')
        safe_upload(target_spread, fetch_ksa, 'FETCH_KSA_TEMPLATE')
        safe_upload(target_spread, qatar, 'FETCH_QATAR_TEMPLATE')
        safe_upload(target_spread, bahrain, 'FETCH_BAHRAIN_TEMPLATE')
        safe_upload(target_spread, sant_tawseel, 'SCENT_PASSION_TEMPLATE')
        safe_upload(target_spread, rt_tawseel, 'RT_TEMPLATE')
        safe_upload(target_spread, lpg_tawseel, 'LPG_TEMPLATE')
        safe_upload(target_spread, oud_tawseel, 'OUD_TEMPLATE')
        safe_upload(target_spread, bsparq_tawseel, 'BSPARQ_TEMPLATE')

        print("--- Sync Completed Successfully ---")

    except Exception as e:
        print(f"CRITICAL ERROR: {e}")


if __name__ == "__main__":
    try:
        while True:
            run_sync()
            # time.sleep(60)
            time.sleep(180)
    except KeyboardInterrupt:
        print("\nShutdown complete.")