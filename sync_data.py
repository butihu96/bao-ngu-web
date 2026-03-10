import gspread
import json
import os
import re
import sys
import traceback
import time  
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pickle

SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# =========================================================
# VŨ KHÍ BÍ MẬT: SỔ ĐEN & NHẬN DIỆN HÃNG
# =========================================================
BLACKLIST_CODES = ["JP7309"] 

CUSTOM_BRAND_MAPPING = {
    "1183A872": "Onitsuka Tiger",     
    "WRS": "Wilson",
    "SKECHER": "Skechers",
    "BABOLAT": "Babolat",
    "WILSON": "Wilson",
    "LACOSTE": "Lacoste",
    "ROGER PRO": "On"  
}

def is_blacklisted(text):
    if not text: return False
    t = str(text).upper()
    for b in BLACKLIST_CODES:
        if b in t: return True
    return False

# =========================================================
# CẤU HÌNH KHO
# =========================================================
SHEETS_CONFIG = [
    {"name": "Kho Điệp Phạm", "id": "1LguFhRHWfHI87onU-Awobfallk_gtZZdPhpCllnK1eo", "type": "kho_1", "col_hang": 2, "col_code": 3, "col_size": 4, "col_price": 5},
    {"name": "Kho LV", "id": "1d1wSARzGqFBmCXOyxR3N8YSoW3oNUHczDmUZOKAGKkE", "type": "kho_2", "left_cols": {"name": 0, "size": 2, "qty": 3, "price": 4}, "right_cols": {"name": 8, "size": 10, "qty": 11, "price": 12}},
    {"name": "Kho Hanaichi (Kho 3)", "id": "1Tiu2VBfxwtACu5wpOTXrNSznoaxBdJj9u3J_WB0uLbc", "type": "kho_3", "col_name_size": 1, "col_price": 2, "col_qty": 6},
    {"name": "Kho Nam Giày (Kho 4)", "id": "1VA_k8EPso5IhBKjQnEUyYVFrQq-nweGwx6AMeYG6Av8", "type": "kho_4"}
]

# =========================================================
# HỆ THỐNG XÁC THỰC
# =========================================================
def get_creds():
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token: 
            creds = pickle.load(token)
            
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token: 
            creds.refresh(Request())
        else:
            if os.getenv("GITHUB_ACTIONS"):
                print("\n❌ LỖI CHÍ MẠNG: Token hết hạn trên Github!\n")
                sys.exit(1) 
            else:
                if not os.path.exists('credentials.json'):
                    print("❌ LỖI: Không tìm thấy file credentials.json!")
                    sys.exit(1)
                flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
        
        with open('token.pickle', 'wb') as token: 
            pickle.dump(creds, token)
            
    return creds

def normalize_key(text):
    if not text: return ""
    return re.sub(r'[^A-Z0-9]', '', str(text).upper())

def clean_size(s):
    s = str(s).replace(',', '.')
    return re.sub(r'\s+', ' ', re.sub(r'[^\d./ -]', '', s)).strip()

def is_valid_size(s):
    if not s: return False
    s = str(s).strip()
    if re.search(r'\d{4}', s): return False
    if re.fullmatch(r'\d{3}', s): return False
    try:
        match = re.search(r'(\d+[.,]?\d*)', s)
        if match:
            num = float(match.group(1).replace(',', '.'))
            if num < 35 or num > 49: return False
    except: return False
    return True

def get_val(row, idx):
    return str(row[idx]).strip() if idx < len(row) else ""

def extract_price(price_str):
    clean_p = price_str.replace('.', '').replace(',', '')
    nums = re.findall(r'\d+', clean_p)
    if not nums: return 0
    return max([int(p) for p in nums])

def la_hang_tap_nham(ten):
    ten = str(ten).upper()
    tu_khoa_cam = ['TẤT', 'VỚ', 'QUẦN', 'ÁO', 'SOCK', 'BALO', 'TÚI', 'MŨ', 'CAP', 'HAT', 'SHIRT', 'PANT']
    for tu in tu_khoa_cam:
        if tu in ten: return True
    return False

def loc_ma_giay(ten_rac):
    ten_rac = str(ten_rac).upper()
    match_on = re.search(r'\b(?=.*[A-Z])(?=.*\d)[A-Z0-9]{11}\b', ten_rac)
    if match_on: return match_on.group(0)
    match_lacoste = re.search(r'\b\d{2,4}[A-Z]{2,4}[A-Z0-9]{5,8}\b', ten_rac)
    if match_lacoste: return match_lacoste.group(0)
    match_babolat = re.search(r'\b[A-Z0-9]{9}-[A-Z0-9]{4}\b', ten_rac)
    if match_babolat: return match_babolat.group(0)
    match_skechers = re.search(r'\b\d{6}[A-Z]?[-/]?[A-Z]{3,4}\b', ten_rac)
    if match_skechers: return match_skechers.group(0)
    match_wilson = re.search(r'\bWRS[A-Z0-9]{4,8}\b', ten_rac)
    if match_wilson: return match_wilson.group(0)
    match_gen = re.search(r'\b[A-Z0-9]{6,8}-[A-Z0-9]{3,4}\b', ten_rac)
    if match_gen: return match_gen.group(0)
    match_code = re.search(r'(?<!-)\b(?=.*\d)(?=.*[A-Z])[A-Z0-9]{6}\b(?!-)', ten_rac)
    if match_code: return match_code.group(0)
    return ten_rac

def nhan_dien_hang(original_name, dict_key):
    name = str(original_name).upper()
    full_str = f"{original_name} {dict_key}".upper()
    for keyword, brand in CUSTOM_BRAND_MAPPING.items():
        if keyword in full_str: return brand
    if re.search(r'\bON\b', full_str) or re.search(r'\b(?=.*[A-Z])(?=.*\d)[A-Z0-9]{11}\b', name): return 'On'
    if re.search(r'\b\d{2,4}[A-Z]{2,4}[A-Z0-9]{5,8}\b', name) or "LACOSTE" in full_str: return 'Lacoste'
    if name.startswith("11") and "-" in name: return 'Onitsuka Tiger'
    if name.startswith("10") and "-" in name: return 'Asics'
    if re.search(r'\b[A-Z0-9]{9}-[A-Z0-9]{4}\b', name): return 'Babolat'
    if re.search(r'\b\d{6}[A-Z]?[-/]?[A-Z]{3,4}\b', name) or "SKECHER" in full_str: return 'Skechers'
    if re.search(r'\bWRS[A-Z0-9]{4,8}\b', name): return 'Wilson'
    if re.search(r'\b[A-Z0-9]{6}-[A-Z0-9]{3}\b', name): return 'Nike'
    if re.search(r'(?<!-)\b(?=.*\d)(?=.*[A-Z])[A-Z0-9]{6}\b(?!-)', name): return 'Adidas'
    return 'Khác'

def sync_data():
    try:
        print("--- Bot đang quét đa kho... ---")
        client = gspread.authorize(get_creds())
        sneaker_dict = {}

        for config in SHEETS_CONFIG:
            try:
                sheet_doc = client.open_by_key(config["id"])
                worksheets = sheet_doc.worksheets()
            except Exception as e: 
                print(f"Bỏ qua kho {config['name']} vì lỗi: {e}")
                continue
            
            for i, ws in enumerate(worksheets):
                if config["type"] == "kho_1" and i in [1, 2]: continue
                if config["type"] == "kho_2" and "onitsuka" in ws.title.lower(): continue
                if config["type"] == "kho_4" and i > 5: continue
                
                data = ws.get_all_values()
                if not data: continue

                # =========================================================
                # KHO 4 - LẤY NGUYÊN VĂN TÊN DÀI SỌC
                # =========================================================
                if config["type"] == "kho_4":
                    blocks = []
                    if i == 0: blocks = [(1, 2, 3), (6, 7, 8), (11, 12, 13)]
                    elif i == 1: blocks = [(0, 1, 2), (4, 5, 6), (9, 10, 11), (14, 15, 16)]
                    elif i in [2, 3]: blocks = [(1, 2, 3), (6, 7, 8), (11, 12, 13)]
                    elif i in [4, 5]: blocks = [(1, 2, 3), (6, 7, 8)]

                    for b_name, b_size, b_price in blocks:
                        current_name = ""
                        current_price = 0
                        
                        for r_idx in range(2, len(data)):
                            row = data[r_idx]
                            name_val = get_val(row, b_name) or get_val(row, b_name - 1)
                            size_val = get_val(row, b_size)
                            price_val = get_val(row, b_price)
                            
                            if name_val and not str(name_val).isdigit():
                                upper_name = name_val.upper()
                                if "HÀNG SẴN" in upper_name or "ĐANG VỀ" in upper_name or "CHÚ Ý" in upper_name:
                                    current_name = ""
                                    current_price = 0
                                    continue
                                
                                if len(name_val) > 5 and not la_hang_tap_nham(name_val):
                                    current_name = name_val
                                    current_price = 0 
                                    
                            if price_val:
                                p_m = extract_price(price_val)
                                if p_m > 0:
                                    if p_m < 100000: p_m = p_m * 1000
                                    current_price = p_m
                                    
                            if current_name and size_val and current_price > 0:
                                # LẤY NGUYÊN VĂN DÒNG TEXT LÀM MÃ SẢN PHẨM LUÔN
                                raw_code = current_name.strip()
                                
                                # Chống lấy phải dòng rác: Phải có ít nhất 1 con số và Không nằm trong Sổ đen
                                if not raw_code or not any(c.isdigit() for c in raw_code) or is_blacklisted(raw_code): 
                                    continue
                                
                                fp = int(round(current_price + 300000, -4))
                                dk = normalize_key(raw_code)
                                
                                size_str = str(size_val).replace(',', '.').replace('\n', ' ')
                                parts = re.split(r'\s+', size_str.strip())
                                
                                for p in parts:
                                    p = p.strip()
                                    if not p: continue
                                    if re.fullmatch(r'\d{3}', p) and p.endswith('5'):
                                        p = p[:2] + '.' + p[2]
                                        
                                    s_c = clean_size(p)
                                    if is_valid_size(s_c):
                                        if dk not in sneaker_dict: 
                                            sneaker_dict[dk] = {"display_name": raw_code, "original_name": raw_code, "variants": {}}
                                        if s_c not in sneaker_dict[dk]["variants"] or fp < sneaker_dict[dk]["variants"][s_c]:
                                            sneaker_dict[dk]["variants"][s_c] = fp

                # =========================================================
                # 3 KHO CŨ
                # =========================================================
                else:
                    for row in data[1:]:
                        try:
                            raw_code = ""
                            price_val = ""
                            s_c = ""

                            if config["type"] == "kho_1":
                                raw_code = get_val(row, config["col_code"]) or get_val(row, config["col_hang"])
                                if la_hang_tap_nham(raw_code) or is_blacklisted(raw_code): continue 
                                
                                price_val = get_val(row, config["col_price"])
                                sizes_raw = get_val(row, config["col_size"]).split('\n')
                                p_max = extract_price(price_val)
                                
                                if p_max == 0 or not raw_code or str(raw_code).isdigit(): continue
                                final_price = int(round((p_max * 1000) + 300000, -4))
                                if final_price < 1000000: continue 
                                
                                dict_key = normalize_key(raw_code)
                                if dict_key not in sneaker_dict: 
                                    sneaker_dict[dict_key] = {"display_name": raw_code.upper(), "original_name": raw_code.upper(), "variants": {}}
                                
                                for s in sizes_raw:
                                    sc = clean_size(s)
                                    if is_valid_size(sc):
                                        if sc not in sneaker_dict[dict_key]["variants"] or final_price < sneaker_dict[dict_key]["variants"][sc]:
                                            sneaker_dict[dict_key]["variants"][sc] = final_price
                                continue

                            elif config["type"] == "kho_3":
                                name_size_val = get_val(row, config["col_name_size"])
                                price_val = get_val(row, config["col_price"])
                                qty_val = get_val(row, config["col_qty"])
                                
                                try: so_luong = int(float(qty_val))
                                except ValueError: so_luong = 0
                                
                                if so_luong < 1: continue

                                if not name_size_val or not price_val or la_hang_tap_nham(name_size_val) or is_blacklisted(name_size_val): continue
                                
                                raw_code = loc_ma_giay(name_size_val)
                                if not raw_code or str(raw_code).isdigit() or is_blacklisted(raw_code): continue
                                
                                size_match = re.search(r'(?:EU|Size|UK|US)\s*([0-9.,/]+)', name_size_val, re.IGNORECASE)
                                if size_match: s_c = clean_size(size_match.group(1))
                                else:
                                    size_match = re.search(r'\(\s*([0-9.,/]+)\s*\)', name_size_val)
                                    if size_match: s_c = clean_size(size_match.group(1))
                                    else:
                                        size_tail = re.search(r'-\s*([0-9]{2}[.,]?[0-9]{0,2})$', name_size_val.strip())
                                        if size_tail: s_c = clean_size(size_tail.group(1))
                                
                                if not is_valid_size(s_c): continue

                            elif config["type"] == "kho_2":
                                continue

                            dict_key = normalize_key(raw_code)
                            p_max = extract_price(price_val)
                            if p_max == 0: continue
                            
                            final_price = int(round((p_max * 1000) + 300000, -4))
                            if final_price < 1000000: continue 
                            
                            if dict_key not in sneaker_dict: 
                                sneaker_dict[dict_key] = {"display_name": raw_code.upper(), "original_name": name_size_val.upper(), "variants": {}}
                            
                            if s_c not in sneaker_dict[dict_key]["variants"] or final_price < sneaker_dict[dict_key]["variants"][s_c]:
                                sneaker_dict[dict_key]["variants"][s_c] = final_price
                        except: continue

                    if config["type"] == "kho_2":
                        def parse_side(cols):
                            blocks = []; curr = {"names": [], "sizes": [], "price_val": 0}
                            for r in data[1:]:
                                n=get_val(r, cols["name"]); s=get_val(r, cols["size"]); q=get_val(r, cols["qty"]); p=get_val(r, cols["price"])
                                if not any([n,s,q,p]):
                                    if curr["names"] or curr["sizes"]:
                                        blocks.append(curr)
                                        curr = {"names": [], "sizes": [], "price_val": 0}
                                    continue
                                if n: curr["names"].append(n)
                                if p:
                                    p_m = extract_price(p)
                                    if p_m > curr["price_val"]: curr["price_val"] = p_m
                                if s and q and str(q).strip().lower() not in ['0','hết', '#n/a', '']:
                                    curr["sizes"].append(s)
                            if curr["names"] or curr["sizes"]: blocks.append(curr)
                            return blocks
                        
                        for b in parse_side(config["left_cols"]) + parse_side(config["right_cols"]):
                            if not b["names"] or b["price_val"] == 0: continue
                            full_text = " ".join(b["names"])
                            if la_hang_tap_nham(full_text) or is_blacklisted(full_text): continue
                            
                            code_c = loc_ma_giay(full_text)
                            if str(code_c).isdigit() or is_blacklisted(code_c): continue
                            
                            fp = int(round((b["price_val"] * 1000) + 300000, -4))
                            if fp < 1000000: continue 
                            
                            dk = normalize_key(code_c)
                            if dk not in sneaker_dict: 
                                sneaker_dict[dk] = {"display_name": code_c.upper(), "original_name": full_text.upper(), "variants": {}}
                                
                            for s in b["sizes"]:
                                sc = clean_size(s)
                                if is_valid_size(sc):
                                    if sc not in sneaker_dict[dk]["variants"] or fp < sneaker_dict[dk]["variants"][sc]:
                                        sneaker_dict[dk]["variants"][sc] = fp

        current_time = int(time.time())
        old_data_map = {}
        
        if os.path.exists('data.json'):
            try:
                with open('data.json', 'r', encoding='utf-8') as f:
                    old_json = json.load(f)
                    for item in old_json:
                        old_data_map[item['name']] = item
            except: pass

        result = []
        for dk, info in sneaker_dict.items():
            if not info["variants"]: continue
            brand = nhan_dien_hang(info["original_name"], dk)
            if brand == 'Khác': continue 
            
            sorted_v = sorted([{"size": k, "price": v, "price_display": f"{v:,}đ"} for k, v in info["variants"].items()], key=lambda x: float(re.search(r'\d+', x["size"]).group(0)) if re.search(r'\d+', x["size"]) else 999)
            disp_name = info["display_name"]
            last_updated = current_time 
            
            if disp_name in old_data_map:
                old_variants = old_data_map[disp_name].get("variants", [])
                if old_variants == sorted_v:
                    last_updated = old_data_map[disp_name].get("updated_at", current_time)
            
            result.append({
                "name": disp_name, 
                "brand": brand, 
                "variants": sorted_v, 
                "updated_at": last_updated
            })

        priority_order = {"Nike": 1, "Adidas": 2, "Asics": 3, "Onitsuka Tiger": 4, "Skechers": 5, "On": 6, "Lacoste": 7, "Babolat": 8, "Wilson": 9}
        result.sort(key=lambda x: (priority_order.get(x["brand"], 99), x["name"]))

        with open('data.json', 'w', encoding='utf-8') as f: json.dump(result, f, ensure_ascii=False, indent=4)
        print(f"✅ Xong! Đã dọn sạch mã rác và ép hiển thị CỤM TÊN DÀI cho Kho 4.")
        
    except Exception as e: 
        print(f"\n❌ LỖI HỆ THỐNG TRẦM TRỌNG: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    sync_data()
