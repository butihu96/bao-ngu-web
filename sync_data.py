import gspread
import json
import os
import re
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import pickle

SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# =========================================================
# TỰ ĐỊNH NGHĨA PHÂN LOẠI HÃNG (VŨ KHÍ BÍ MẬT)
# =========================================================
CUSTOM_BRAND_MAPPING = {
    "1183A872": "Asics",     
    "WRS": "Hãng Khác"
}

# =========================================================
# BẢN ĐỒ CẤU HÌNH ĐA TỔNG KHO 
# =========================================================
SHEETS_CONFIG = [
    {
        "name": "Kho Điệp Phạm",
        "id": "1LguFhRHWfHI87onU-Awobfallk_gtZZdPhpCllnK1eo",
        "type": "kho_1",
        "col_hang": 2, "col_code": 3, "col_size": 4, "col_price": 5
    },
    {
        "name": "Kho LV",
        "id": "1d1wSARzGqFBmCXOyxR3N8YSoW3oNUHczDmUZOKAGKkE", 
        "type": "kho_2",
        "left_cols":  {"name": 0, "size": 2, "qty": 3, "price": 4},
        "right_cols": {"name": 8, "size": 10, "qty": 11, "price": 12}
    },
    {
        "name": "Kho Hanaichi (Kho 3)",
        "id": "1Tiu2VBfxwtACu5wpOTXrNSznoaxBdJj9u3J_WB0uLbc", 
        "type": "kho_3",
        "col_name_size": 1, 
        "col_price": 2,     
        "col_qty": 6        
    }
]

def get_creds():
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
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

def get_val(row, idx):
    return str(row[idx]).strip() if idx < len(row) else ""

def extract_price(price_str):
    clean_p = price_str.replace('.', '').replace(',', '')
    nums = re.findall(r'\d+', clean_p)
    if not nums: return 0
    return max([int(p) for p in nums])

# =========================================================
# BỘ NHẬN DIỆN HÃNG THEO FORMAT MÃ CODE (REGEX) 
# =========================================================
def nhan_dien_hang(display_name, dict_key):
    name = str(display_name).upper()
    full_str = f"{display_name} {dict_key}".upper()

    # 1. Quét theo Từ Điển Tự Tạo trước
    for keyword, brand in CUSTOM_BRAND_MAPPING.items():
        if keyword.upper() in full_str:
            return brand

    # 2. ASICS: Dạng 8 ký tự - 3 ký tự (VD: 1041A481-003)
    if re.search(r'\b[A-Z0-9]{8}-[A-Z0-9]{3}\b', name):
        return 'Asics'
        
    # 3. NIKE: Dạng 6 ký tự - 3 ký tự (VD: DR6192-101)
    if re.search(r'\b[A-Z0-9]{6}-[A-Z0-9]{3}\b', name):
        return 'Nike'
        
    # 4. ADIDAS (DAS): Dạng 6 ký tự (VD: IH8158)
    if re.search(r'(?<!-)\b(?=.*\d)[A-Z0-9]{6}\b(?!-)', name):
        return 'Adidas'
        
    return 'Khác'

def sync_data():
    try:
        print(f"--- Đang khởi động Bot quét {len(SHEETS_CONFIG)} kho hàng... ---")
        client = gspread.authorize(get_creds())
        sneaker_dict = {}

        for config in SHEETS_CONFIG:
            sheet_id = config["id"]
            if "DÁN_ID" in sheet_id: continue
            
            print(f"\n-> ĐANG CHUI VÀO KHO: {config['name']}")
            try:
                sheet_doc = client.open_by_key(sheet_id)
                worksheets = sheet_doc.worksheets()
            except Exception as e:
                print(f"   [!] Lỗi mở kho: {e}")
                continue
            
            for ws in worksheets:
                data = ws.get_all_values()
                if not data: continue
                count_items = 0 

                # ==================================
                # KHO 1: DÒNG ĐƠN
                # ==================================
                if config["type"] == "kho_1":
                    for row in data[1:]:
                        try:
                            hang = get_val(row, config["col_hang"])
                            code = get_val(row, config["col_code"])
                            raw_code = code if code else hang
                            if not raw_code or raw_code.lower() == 'nan': continue
                            
                            dict_key = normalize_key(raw_code)
                            p_max = extract_price(get_val(row, config["col_price"]))
                            if p_max == 0: continue
                            final_price = int(round((p_max * 1000) + 300000, -4))

                            if dict_key not in sneaker_dict:
                                sneaker_dict[dict_key] = {"display_name": raw_code.upper(), "variants": {}}
                            
                            sizes = get_val(row, config["col_size"]).split('\n')
                            for s in sizes:
                                s_c = clean_size(s)
                                if s_c:
                                    count_items += 1
                                    if s_c not in sneaker_dict[dict_key]["variants"] or final_price < sneaker_dict[dict_key]["variants"][s_c]:
                                        sneaker_dict[dict_key]["variants"][s_c] = final_price
                        except: continue

                # ==================================
                # KHO 2: KHỐI ẢNH
                # ==================================
                elif config["type"] == "kho_2":
                    def parse_side(cols):
                        blocks = []
                        curr_block = None
                        empty_names_count = 0
                        for row in data[1:]:
                            n_val = get_val(row, cols["name"])
                            s_val = get_val(row, cols["size"])
                            q_val = get_val(row, cols["qty"])
                            p_val = get_val(row, cols["price"])
                            if not any([n_val, s_val, q_val, p_val]): continue
                            
                            if n_val:
                                if curr_block is None or empty_names_count > 0:
                                    if curr_block: blocks.append(curr_block)
                                    curr_block = {"names": [], "sizes": [], "price_val": 0}
                                curr_block["names"].append(n_val)
                                empty_names_count = 0
                            else:
                                empty_names_count += 1
                                
                            if curr_block:
                                p_max = extract_price(p_val)
                                if p_max > curr_block["price_val"]: curr_block["price_val"] = p_max
                                if s_val and q_val and str(q_val).strip() not in ['0', 'hết', '']:
                                    curr_block["sizes"].append(s_val)
                        if curr_block: blocks.append(curr_block)
                        return blocks

                    for b in parse_side(config["left_cols"]) + parse_side(config["right_cols"]):
                        if not b["names"] or b["price_val"] == 0: continue
                        full_text = " ".join(b["names"]).strip()
                        code_cand = next((w for w in reversed(full_text.split()) if any(c.isdigit() for c in w)), full_text)
                        
                        dict_key = normalize_key(code_cand)
                        final_price = int(round((b["price_val"] * 1000) + 300000, -4))
                        
                        if dict_key not in sneaker_dict:
                            sneaker_dict[dict_key] = {"display_name": code_cand.upper(), "variants": {}}
                        for s in b["sizes"]:
                            s_c = clean_size(s)
                            if s_c:
                                count_items += 1
                                if s_c not in sneaker_dict[dict_key]["variants"] or final_price < sneaker_dict[dict_key]["variants"][s_c]:
                                    sneaker_dict[dict_key]["variants"][s_c] = final_price

                # ==================================
                # KHO 3: ĐÃ FIX CẮT ĐUÔI VÀ BỌC THÉP
                # ==================================
                elif config["type"] == "kho_3":
                    curr_name_colA = "" 
                    for row in data[1:]:
                        try:
                            col_a_val = get_val(row, 0)
                            if col_a_val: curr_name_colA = col_a_val

                            name_size_val = get_val(row, config["col_name_size"])
                            price_val = get_val(row, config["col_price"])
                            
                            qty_col_E = get_val(row, 4) 
                            qty_col_G = get_val(row, 6) 

                            if not name_size_val or not price_val: continue
                            
                            has_stock = False
                            for q in [qty_col_E, qty_col_G]:
                                if q and str(q).strip() not in ['0', 'hết', '#N/A', '#VALUE!', '']:
                                    has_stock = True
                                    break
                            if not has_stock: continue

                            s_c = ""
                            size_match = re.search(r'(?:EU|Size|UK|US)\s*([0-9.,/ ]+)', name_size_val, re.IGNORECASE)
                            if size_match:
                                s_c = clean_size(size_match.group(1))
                            else:
                                size_match = re.search(r'\(\s*([0-9.,/ ]+)\s*\)', name_size_val)
                                if size_match: s_c = clean_size(size_match.group(1))

                            if not s_c: continue

                            raw_code = re.split(r'\(|EU|Size', name_size_val, flags=re.IGNORECASE)[0].strip()
                            parts = raw_code.split('-')
                            
                            if len(parts) >= 3 and parts[-1].strip().isdigit() and 2 <= len(parts[-1].strip()) <= 3:
                                code_part = '-'.join(parts[:-1]).strip()
                            elif len(parts) == 2 and raw_code.find(' - ') != -1:
                                code_part = parts[0].strip()
                            else:
                                code_part = raw_code

                            if not code_part: continue

                            dict_key = normalize_key(code_part)
                            p_max = extract_price(price_val)
                            if p_max == 0: continue
                            final_price = int(round((p_max * 1000) + 300000, -4))

                            display_name = f"{curr_name_colA} {code_part}".strip() if curr_name_colA else code_part

                            if dict_key not in sneaker_dict:
                                sneaker_dict[dict_key] = {"display_name": display_name, "variants": {}}

                            count_items += 1
                            if s_c not in sneaker_dict[dict_key]["variants"] or final_price < sneaker_dict[dict_key]["variants"][s_c]:
                                sneaker_dict[dict_key]["variants"][s_c] = final_price

                        except Exception:
                            continue
                
                print(f"      + Soi Tab: [{ws.title}] - Bắt được {count_items} size hợp lệ")

        # ==================================
        # ĐÓNG GÓI VÀ PHÂN LOẠI XUẤT RA WEB
        # ==================================
        result = []
        
        # Bọc thép chống lỗi sắp xếp size dị (như 39.5-40)
        def sort_key(s):
            try:
                match = re.search(r'(\d+[.,]?\d*)', str(s))
                return float(match.group(1).replace(',', '.')) if match else 999
            except:
                return 999

        for dict_key, info in sneaker_dict.items():
            if not info["variants"]: continue
            sorted_v = []
            
            s_keys = sorted(info["variants"].keys(), key=sort_key)
            
            for sk in s_keys:
                sorted_v.append({
                    "size": sk,
                    "price": info["variants"][sk],
                    "price_display": f"{info['variants'][sk]:,}đ"
                })
            
            brand = nhan_dien_hang(info["display_name"], dict_key)
            result.append({
                "name": info["display_name"], 
                "brand": brand, 
                "variants": sorted_v
            })

        thu_tu = {"Nike": 1, "Adidas": 2, "Asics": 3, "Khác": 4}
        result.sort(key=lambda x: (thu_tu.get(x["brand"], 4), x["name"]))

        with open('data.json', 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=4)
        print(f"\n✅ ĐÃ XONG TẤT CẢ! Gom được tổng cộng {len(result)} mẫu giày, đã phân loại Hãng xịn xò.")

    except Exception as e:
        print(f"\n❌ Lỗi hệ thống: {e}")

if __name__ == "__main__":
    sync_data()
    # Chống tắt màn hình CMD để đọc lỗi
    input("\n[!] TOOL ĐÃ DỪNG LẠI. Nhấn phím Enter để tắt cửa sổ này...")