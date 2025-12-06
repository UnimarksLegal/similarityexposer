import fitz , re,requests,os
import pandas as pd
from dotenv import load_dotenv
records = []
def extract_govt_pdf(pdffilename)->pd.DataFrame:
    
    pdf = fitz.open(pdffilename)
    for page in pdf:
        width , height = page.rect.width, page.rect.height
        blocks = page.get_text('blocks')

        Header_y_max = 100
        Footer_y_min = height-150

        header_text = []
        middle_text = []
        footer_text = []

        for (x0,y0,x1,y1,text,*_rest) in blocks:
            t = text.replace('\xa0','').strip()
            if not t:
                continue
            if y0 < Header_y_max:
                header_text.append(t)
            elif y0> Footer_y_min:
                footer_text.append(t)
            else:
                middle_text.append(t)

        header_txt = " ".join(header_text)
        middle_txt = "\n".join(middle_text)
        footer_txt = " ".join(footer_text)

        # 1) class
        class_no = re.search(r'Class\s+(\d+)', header_txt, re.IGNORECASE)
        real_class = class_no.group(1) if class_no else None

        # 2) app no + date
        app = re.search(r"(\d{7})\s*(\d{2}/\d{2}/\d{4})", middle_txt)
        app_no = app.group(1) if app else None
        app_date = app.group(2) if app else None

        # 3) mark name from header
        brand_middle = re.search(r'Class\s+\d+\s+(.+)', header_txt, re.IGNORECASE)
        brand_name = brand_middle.group(1).strip() if brand_middle else 'Image'

        # 4) page number
        pageno = re.search(r"\d+", footer_txt)
        page_no = pageno.group(0) if pageno else None

        # 5) Company Name
        cp_name = re.search(r"\d+/\d+/\d+\s+(.*)",middle_txt,re.IGNORECASE)
        com_name = cp_name.group(1).strip() if cp_name else None

        # 6) Goods

        cities = ["DELHI", "MUMBAI", "CHENNAI", "KOLKATA", "AHMEDABAD"]

        lines = [ln.strip() for ln in middle_txt.splitlines() if ln.strip()]

        # indices where the whole line is exactly a city (not in an address line)
        city_idx_list = [i for i, ln in enumerate(lines) if ln in cities]

        goods = None
        if city_idx_list:
            last_city_idx = city_idx_list[-1]          # use the last standalone city
            goods_lines = lines[last_city_idx + 1:]    # everything after that
            if goods_lines:
                goods = " ".join(goods_lines).strip()

        row = {
        "appno": app_no,
        "class": real_class,
        "tmAppliedFor": brand_name,
        "buisnessName" : com_name,
        "goodsAndSerice" : goods,
        "dateOfApp": app_date,
        "page_no": page_no}
            
        records.append(row)

    govt = pd.DataFrame(records)
    govt['page_no'] = govt['page_no'].fillna(0)
    govt['goodsAndSerice'] = govt['goodsAndSerice'].fillna('Refer Pdf')
    govt['page_no'] = pd.to_numeric(govt['page_no'], errors='coerce')
    # govt['page_no'] = govt['page_no'].astype('Int64')
    govt['appno'] = pd.to_numeric(govt['appno'], errors='coerce')
    govt = govt.dropna()
    print('Govt df Created')
    return govt

def prepare_tmpilot(excel)->pd.DataFrame:

    tmpilot = pd.read_excel(excel,index_col=False)
    needed_cols = ['appno','class', 'tmAppliedFor','buisnessName', 'goodsAndSerice','dateOfApp','propName','country', 'JournalDate']
    # tmpilot['appno'] = tmpilot['appno'].astype('Int64')
    tmpilot = tmpilot[needed_cols]
    tmpilot['appno'] = pd.to_numeric(tmpilot['appno'], errors='coerce')
    print('TM-Pilot df Created')
    return tmpilot

import re
import pandas as pd

REMOVE_PHRASES = [
    "PRIVATE LIMITED","GROUP","FOODS","BRAND","MARK","PVT LTD","WITH DEVICE",
    "WITH THE DEVICE","WITH DEVICE","LTD","WITH LABEL AND LOGO","WITH LABEL",
    "COMPANY","ENTERPRISES","ENTERPRISE","LIMITED","LLP","INDIA","SOLUTIONS",
    "SERVICES","UNIT","HOSPITAL","HOSPITALS","(LABEL)","LABEL","PHARMA","PHARMACEUTICALS",
    "PHARMACY","STUDIOS","STUDIO","MASALA","(IN HINDI)","ORGANIC","ORGANICS",
    "HEALTHCARE","ENTERTAINMENT","ENTERTAINMENTS","CARE","CARES","JEWELS",
    "AGRO","VALUE","VALUES","TECH","TECHNOLOGIES","TECHNOLOGY","PRODUCTS",
    "PRODUCT","CLINICS","CLINIC","GOLD","GOLDS","GRANULES","SPORTS","GLOBAL",
    "DEVICE","LOGO","SCHOOL","JEWELLERS","JEWELLERY","THE"," CINEMAS","    CINEMAS"," PHARMACEUTICAL"," PHARMACEUTICALS",
    " GROUP OF COMPANIES","A (LABEL)","AMP","A","B","C","D","E","F","G","H","I","J",
    "K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z","b (DEVICE)","A (LABEL)"," CONSTRUCTIONS"," PUBLICATIONS"
]

PHRASES_RE = re.compile(
    r"\b(?:" + "|".join(re.escape(p.strip()) for p in REMOVE_PHRASES if p.strip()) + r")\b"
)

def clean_brand(s) -> str:
    if pd.isna(s):
        return ""
    s = str(s).upper()

    # remove leading pure DEVICE marks
    if re.match(r"^(DEVICE(\s+OF\b.*)?|[A-Z]\s+DEVICE\b.*)", s):
        return ""

    # remove (DEVICE ...) suffixes
    s = re.sub(r"\(\s*DEVICE[^\)]*\)", " ", s)

    # remove boilerplate phrases (including LOGO now)
    s = PHRASES_RE.sub(" ", s)

    # keep only letters/digits/spaces
    s = re.sub(r"[^A-Z0-9 ]+", " ", s)

    # collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()

    return s

# build a single regex that removes any of those phrases as whole words
PHRASES_RE = re.compile(
    r"\b(?:"
    + "|".join(re.escape(p.strip()) for p in REMOVE_PHRASES if p.strip())
    + r")\b"
)

def clean_class(x):
    if pd.isna(x):
        return None
    x = str(x).strip()
    m = re.search(r"(\d+)", x)
    return m.group(1) if m else None

load_dotenv()


CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
REFRESH_TOKEN = os.getenv('REFRESH_TOKEN')


API_DOMAIN = "https://www.zohoapis.com"
OWNER_NAME = os.getenv('OWNER_NAME')
APP_LINK_NAME = "tmdb-new"
REPORT_LINK_NAME = "All_Tmdb_Copies"

def get_access_token() -> str:
    url = "https://accounts.zoho.com/oauth/v2/token"
    data = {
        "grant_type": "refresh_token",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": REFRESH_TOKEN,
    }
    resp = requests.post(url, data=data, timeout=30)
    resp.raise_for_status()
    return resp.json()["access_token"]

def fetch_all_brands():
    access_token = get_access_token()
    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}"
    }

    records = []
    record_cursor = None

    while True:
        url = f"{API_DOMAIN}/creator/v2.1/data/{OWNER_NAME}/{APP_LINK_NAME}/report/{REPORT_LINK_NAME}"
        params = {
            "max_records": 1000,   # 200/500/1000 allowed
        }
        if record_cursor:
            headers["record_cursor"] = record_cursor

        resp = requests.get(url, headers=headers, params=params, timeout=30)

        if resp.status_code != 200:
            print("Status:", resp.status_code)
            print("URL:", resp.url)
            print("Body:", resp.text)
            resp.raise_for_status()

        data = resp.json()
        batch = data.get("data", [])
        records.extend(batch)

        # Pagination via response header
        record_cursor = resp.headers.get("record_cursor")
        if not record_cursor:
            break

    return records

# ZOHO
def prepare_zoho(brand)->pd.DataFrame:
    zoho = pd.DataFrame(brand)
    zoho['Current_Status'] = zoho['Current_Status'].fillna('Unknown')
    zoho = zoho[~zoho['Current_Status'].isin(['Withdrawn','Refused','Abandoned'])]
    zoho = zoho[['Application_No','Class','Trademark','Company_Name1','Company_Name','Goods_38_Services','Client_Name','Journal_Date']]
    zoho = zoho.rename(columns={'Application_No':'zoho_appno','Class':'zohoclass','Trademark':'zoho_tm',
                                'Company_Name1':'zoho_cmp1','Company_Name':'zoho_cmp2',
                                'Goods_38_Services':'zoho_goods','Client_Name':'our_client','Journal_Date':'JournalDate'})
    zoho['zoho_tm'] = zoho['zoho_tm'].fillna('NULLs')
    zoho = zoho[~zoho['zoho_tm'].isin(['LOGO','DEVICE','(DEVICE)','(LOGO)','( DEVICE )','(DEVICE OF FINGER IMPRESSION)'])]
    zoho["norm_tm"] = zoho["zoho_tm"].apply(clean_brand)
    zoho = zoho[zoho["norm_tm"] != ""]
    zoho['zohoclass'] = zoho['zohoclass'].apply(clean_class)
    # zoho['zohoclass'] = zoho['zohoclass'].astype(str)
    print('Zoho df Created')
    return zoho 
