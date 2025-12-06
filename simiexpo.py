import pandas as pd
# from datetime import datetime
# from rapidfuzz import fuzz,process
# from zohoscraper import fetch_all_brands
from prepareeve import extract_govt_pdf , prepare_tmpilot , clean_brand, clean_class

# finalpdf = fitz.open()
# m1 = fitz.open('27.10.1.pdf')
# m1.delete_pages(from_page = 0 , to_page=9)
# m2 = fitz.open('27.10.2.pdf')
# finalpdf.insert_pdf(m1)
# finalpdf.insert_pdf(m2)
# finalpdf.save('finalfull.pdf')
# m1.close()
# m2.close()
# finalpdf.close()

# brands = fetch_all_brands()
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

# # CALLING FUNCTIONS
# zoho_df = prepare_zoho(brands)

# # GOVT PDF PROCESS

# govt_pdf_df = extract_govt_pdf('finalfull.pdf')

# # TM PILOT EXCEL

# tmpilot_df = prepare_tmpilot('27full.xlsx')

# # MISSING IN TM-PILOT
# missing = govt_pdf_df[~govt_pdf_df['appno'].isin(tmpilot_df['appno'])]
# # missing['appno'] = pd.to_numeric(missing['appno'], errors='coerce')
# if missing.shape[0] >0:
#     print('Ooops! Tm pilot missed some Brands from Pdf, check Missing_in_tmpilit file')
#     missing.to_csv('Missing_in_tmpilot.csv',index=0)

# #MERGING MISSING WITH TMPILOT
# tmpilot = tmpilot_df.merge(govt_pdf_df[['appno','page_no']],on='appno',how='left')
# concatenated = pd.concat([tmpilot,missing],ignore_index=1)
# concatenated["norm_tmp"] = concatenated["tmAppliedFor"].apply(clean_brand)
# concatenated = concatenated[concatenated["norm_tmp"] != ""]
# concatenated['class'] = concatenated['class'].apply(clean_class)
# journal_date = pd.to_datetime(concatenated['JournalDate'].iloc[0],format='%d/%m/%Y').strftime('%d-%m-%Y')


# # REAL SIMILARITY EXPOSER ENGINE

# limitt = 4
# thresh_score = 85
# results = []

# for cls,con in concatenated.groupby("class"):
#     zoho_brands = zoho_df[zoho_df['zohoclass']==cls]
    
#     if zoho_brands.empty:
#         continue
#     choices = dict(zip(zoho_brands.index , zoho_brands['norm_tm']))
    
#     for _,crow in con.iterrows():
#         c_name = crow['norm_tmp']
#         c_app_no = crow['appno']
#         c_raw_name = crow['tmAppliedFor']
#         c_company = crow['buisnessName']
#         c_pageno = crow['page_no']
#         c_jd = crow['JournalDate']
#         c_guds = crow['goodsAndSerice']
        
#         matches = process.extract(c_name,choices,
#                                   scorer=fuzz.token_set_ratio,
#                                   limit = limitt,score_cutoff=thresh_score)
#         for brand,score,zoho_idx in matches:
#             if not isinstance(zoho_idx, (int,np.integer)):
#                 print("WEIRD KEY:", zoho_idx)
#                 continue
            
#             zzrow = zoho_brands.loc[zoho_idx]
#             results.append({
#                 "govt_app_no": c_app_no,
#                 "govt_brand": c_raw_name,
#                 "zoho_brand": zzrow["zoho_tm"],
#                 "govt_class": cls,
#                 "zoho_class":zzrow['zohoclass'],
#                 "zoho_client": zzrow.get("our_client"),
#                 "zoho_client_company1": zzrow.get("zoho_cmp1"),
#                 "zoho_client_company2": zzrow.get("zoho_cmp2"),
#                 "zoho_Application_no": zzrow.get("zoho_appno"),
#                 "Compared_zoho_name": zzrow["norm_tm"],
#                 "Compared_govt_name":c_name,
#                 'Govt_company_name':c_company,
#                 "score": score,
#                 "Journal_Date":c_jd,
#                 "Govt_Goods":c_guds,
#                 "Zoho_goods":zzrow.get("zoho_goods"),
#                 "Govt_pdf_pageno":c_pageno
#                 })
# matches_df = pd.DataFrame(results)
# matches_df = matches_df.sort_values(by='score',ascending=0)
# date = datetime.today().date().day
# day = datetime.today().strftime('%A')
# matches_df.to_csv(f'Compared_Journal_of_{journal_date}.csv',index=False)
# print('Process Completed')