import streamlit as st ,  pandas as pd , tempfile,os
from io import BytesIO
from datetime import datetime
from prepareeve import extract_govt_pdf, prepare_tmpilot, clean_brand, clean_class,fetch_all_brands,prepare_zoho

from rapidfuzz import fuzz, process
import numpy as np

# ---------------------------------------------------
# Streamlit Page Settings
# ---------------------------------------------------
st.set_page_config(
    page_title="Trademark Similarity Exposer",
    layout="wide"
)

st.title("Trademark Similarity Exposer Dashboard")
st.caption("Govt Journal × TM-Pilot × Zoho — Similarity Detection Engine")

st.markdown("---")

# Upload widgets
col1, col2, col3 = st.columns(3)

with col1:
    pdf_file_1 = st.file_uploader("Upload Govt PDF 1", type=["pdf"])

with col2:
    pdf_file_2 = st.file_uploader("Upload Govt PDF 2", type=["pdf"])

with col3:
    tmpilot_file = st.file_uploader("Upload TM-Pilot Full-Download Excel", type=["xlsx", "xls"])

st.markdown("---")

start = st.button("Start Processing", type="primary")

# Session state for results
if "matches_df" not in st.session_state:
    st.session_state["matches_df"] = None


# ---------------------------------------------------
# START PIPELINE
# ---------------------------------------------------
if start:

    # -----------------------
    # VALIDATION
    # -----------------------
    if pdf_file_1 is None:
        st.error("Please upload at least Govt PDF 1.")
        st.stop()

    if tmpilot_file is None:
        st.error("Please upload TM-Pilot Excel.")
        st.stop()

    # -----------------------
    # TEMP DIRECTORY FOR PDFs
    # -----------------------
    temp_dir = tempfile.mkdtemp(prefix="simi_")

    # Save PDF 1
    temp_pdf1 = os.path.join(temp_dir, "part1.pdf")
    with open(temp_pdf1, "wb") as f:
        f.write(pdf_file_1.read())

    pdf_paths = [temp_pdf1]

    # Save PDF 2 if uploaded
    if pdf_file_2 is not None:
        temp_pdf2 = os.path.join(temp_dir, "part2.pdf")
        with open(temp_pdf2, "wb") as f:
            f.write(pdf_file_2.read())
        pdf_paths.append(temp_pdf2)

    # -----------------------
    # LOAD ZOHO DATA
    # -----------------------
    with st.spinner("Fetching Zoho data..."):
        brands = fetch_all_brands()
        zoho_df = prepare_zoho(brands)  # your existing function
        st.success(f"Zoho DF Created — {len(zoho_df):,} rows")

    # -----------------------
    # BUILD GOVT DF
    # -----------------------

        # Combine PDFs and parse with your existing scraper
        # You already use extract_govt_pdf('finalfull.pdf')
        # So here we merge manually and pass to your parser.
    with st.spinner("Parsing Govt PDFs..."):
        import fitz

        merged = fitz.open()

        for idx, p in enumerate(pdf_paths):
            tmp = fitz.open(p)

            # delete first 10 pages only from the FIRST journal
            if idx == 0:
                try:
                    tmp.delete_pages(from_page=0, to_page=9)
                except Exception:
                    # if journal has <10 pages, just ignore
                    pass

            merged.insert_pdf(tmp)
            tmp.close()

        temp_full_pdf = os.path.join(temp_dir, "finalgovt.pdf")
        merged.save(temp_full_pdf)
        merged.close()

        govt_pdf_df = extract_govt_pdf(temp_full_pdf)
        st.success(f"Govt DF Created — {len(govt_pdf_df):,} rows")


    # -----------------------
    # PREP TM-PILOT DF
    # -----------------------
    with st.spinner("Loading TM-Pilot Excel..."):
        tmpilot_df = prepare_tmpilot(tmpilot_file)
        st.success(f"TM-Pilot DF Created — {len(tmpilot_df):,} rows")

    # -----------------------
    # FIND MISSING IN TM-PILOT
    # -----------------------
    missing = govt_pdf_df[~govt_pdf_df["appno"].isin(tmpilot_df["appno"])]
    # if missing.shape[0] > 0:
    #     st.warning(f"TM-Pilot missed {len(missing)} records")

    # # Display & download missing records
    # st.markdown("### Missing Records (Govt Not Found in TM-Pilot)")
    if missing.shape[0] == 0:
        st.success("No records were missed by TM-Pilot.")
    else:
        st.warning(f"TM-Pilot missed {len(missing)} records.")
        
        # Show a small preview
        st.dataframe(missing.head(50))

        # Build memory Excel
        missing_buf = BytesIO()
        with pd.ExcelWriter(missing_buf, engine="xlsxwriter") as writer:
            missing.to_excel(writer, index=False, sheet_name="Missing")

            # Format header row
            workbook = writer.book
            worksheet = writer.sheets["Missing"]
            header_fmt = workbook.add_format({"bold": True})
            for col_idx, col_name in enumerate(missing.columns):
                worksheet.write(0, col_idx, col_name, header_fmt)

            worksheet.freeze_panes(1, 0)

        missing_buf.seek(0)

        st.download_button(
            "Download Missing Records",
            data=missing_buf,
            file_name="TMpilot_Missing_Records.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )


    # Merge TM-Pilot + Missing
    tmpilot = tmpilot_df.merge(govt_pdf_df[['appno', 'page_no']], on='appno', how='left')
    concatenated = pd.concat([tmpilot, missing], ignore_index=True)
    concatenated["norm_tmp"] = concatenated["tmAppliedFor"].apply(clean_brand)
    concatenated = concatenated[concatenated["norm_tmp"] != ""]
    concatenated["class"] = concatenated["class"].apply(clean_class)

    # -----------------------
    # RUN SIMILARITY ENGINE
    # -----------------------
    with st.spinner("Running Similarity Engine..."):

        limitt = 4
        thresh_score = 85
        results = []

        for cls, con in concatenated.groupby("class"):
            zoho_brands = zoho_df[zoho_df["zohoclass"] == cls]

            if zoho_brands.empty:
                continue

            choices = dict(zip(zoho_brands.index, zoho_brands["norm_tm"]))

            for _, crow in con.iterrows():
                c_name = crow["norm_tmp"]
                c_app_no = crow["appno"]
                c_raw_name = crow["tmAppliedFor"]
                c_company = crow["buisnessName"]
                c_pageno = crow["page_no"]
                c_jd = crow["JournalDate"]
                c_guds = crow["goodsAndSerice"]

                matches = process.extract(
                    c_name,
                    choices,
                    scorer=fuzz.token_set_ratio,
                    limit=limitt,
                    score_cutoff=thresh_score
                )

                for _, score, zoho_idx in matches:
                    if not isinstance(zoho_idx, (int, np.integer)):
                        continue

                    zz = zoho_brands.loc[zoho_idx]

                    results.append({
                        "govt_app_no": c_app_no,
                        "govt_brand": c_raw_name,
                        "zoho_brand": zz["zoho_tm"],
                        "govt_class": cls,
                        "zoho_class": zz["zohoclass"],
                        "zoho_client": zz.get("our_client"),
                        "zoho_company1": zz.get("zoho_cmp1"),
                        "zoho_company2": zz.get("zoho_cmp2"),
                        "zoho_Application_no": zz.get("zoho_appno"),
                        "Compared_zoho_name": zz["norm_tm"],
                        "Compared_govt_name": c_name,
                        "Govt_company_name": c_company,
                        "score": score,
                        "Journal_Date": c_jd,
                        "Govt_Goods": c_guds,
                        "Zoho_goods": zz.get("zoho_goods"),
                        "Govt_pdf_pageno": c_pageno,
                    })

        matches_df = pd.DataFrame(results).sort_values(by="score", ascending=False)
        st.session_state["matches_df"] = matches_df

        st.success(f"Process Completed — {len(matches_df):,} matches flagged")

    # -----------------------
    # CLEAN TEMP FILES
    # -----------------------
    for file in os.listdir(temp_dir):
        try:
            os.remove(os.path.join(temp_dir, file))
        except:
            pass

from datetime import datetime

# define a default first (e.g., run date)
journal_date = datetime.now().strftime("%d-%m-%Y")

# then try to override from data
try:
    jd_raw = concatenated["JournalDate"].dropna().iloc[0]
    journal_date = pd.to_datetime(jd_raw, format="%d/%m/%Y").strftime("%d-%m-%Y")
except Exception:
    # silently keep default if anything goes wrong
    pass

# ---------------------------------------------------
# RESULTS SECTION
# ---------------------------------------------------
st.markdown("---")
st.subheader("Output")

matches_df = st.session_state.get("matches_df")

if matches_df is not None and not matches_df.empty:
    # st.dataframe(matches_df.head(50))

    # Build styled Excel in memory
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        sheet_name = "Matches"
        matches_df.to_excel(writer, index=False, sheet_name=sheet_name)

        workbook  = writer.book
        worksheet = writer.sheets[sheet_name]

        # Bold header format
        header_format = workbook.add_format({"bold": True})
        for col_idx, col_name in enumerate(matches_df.columns):
            worksheet.write(0, col_idx, col_name, header_format)

        # Freeze header row
        worksheet.freeze_panes(1, 0)

    buf.seek(0)

    st.download_button(
        "Download Complete Excel",
        data=buf,
        file_name=f"Similarity_Report_{journal_date}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
else:
    st.info("Upload files & click Start to begin.")
