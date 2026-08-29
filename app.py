import os
import tempfile
from io import BytesIO
from datetime import datetime
from matching import normalise, run_similarity
import streamlit as st
import pandas as pd

from prepareeve import (
    extract_govt_pdf,
    prepare_tmpilot,
    clean_class,
    fetch_all_brands,
    prepare_zoho,
)
# from matching import normalise
# from compare import run_comparison


# ---------------------------------------------------
# PAGE SETTINGS
# ---------------------------------------------------
st.set_page_config(page_title="Trademark Similarity Exposer", layout="wide")

st.title("Trademark Similarity Exposer")
st.caption("Govt Journal × TM-Pilot × Zoho — Similarity Detection Engine")
st.markdown("---")


# ---------------------------------------------------
# SESSION STATE
# ---------------------------------------------------
# for key in ("matches_df", "cmp_bytes", "dropped_count", "journal_date"):
#     if key not in st.session_state:
#         st.session_state[key] = None
for key in ("matches_df", "journal_date"):
    if key not in st.session_state:
        st.session_state[key] = None


# ---------------------------------------------------
# UPLOADS
# ---------------------------------------------------
col1, col2, col3, col4 = st.columns(4)

with col1:
    tmpilot_file = st.file_uploader(
        "Upload TM-Pilot Full-Download Excel :red[*]", type=["xlsx", "xls"]
    )
with col2:
    pdf_file_1 = st.file_uploader("Upload Govt PDF 1 :red[*]", type=["pdf"])
with col3:
    pdf_file_2 = st.file_uploader("Upload Govt PDF 2", type=["pdf"])
with col4:
    pdf_file_3 = st.file_uploader("Upload Govt PDF 3 (Optional)", type=["pdf"])

st.markdown("---")

start = st.button("Start Processing", type="primary")


# ---------------------------------------------------
# PROCESSING
# ---------------------------------------------------
if start:

    st.cache_data.clear()
    st.cache_resource.clear()

    if tmpilot_file is None:
        st.error("Please upload the TM-Pilot Excel.")
        st.stop()

    if pdf_file_1 is None:
        st.error("Please upload at least Govt PDF 1.")
        st.stop()

    # -----------------------------------------------
    # 1. PARSE GOVT PDFs
    # -----------------------------------------------
    with tempfile.TemporaryDirectory() as temp_dir:
        pdf_paths = []

        for idx, upload in enumerate([pdf_file_1, pdf_file_2, pdf_file_3], start=1):
            if upload is None:
                continue
            path = os.path.join(temp_dir, f"part{idx}.pdf")
            with open(path, "wb") as fh:
                fh.write(upload.read())
            pdf_paths.append(path)

        with st.spinner("Parsing Govt PDFs..."):
            import fitz

            merged = fitz.open()
            for idx, path in enumerate(pdf_paths):
                tmp = fitz.open(path)
                # strip the 10-page preamble from the first journal only
                if idx == 0:
                    try:
                        tmp.delete_pages(from_page=0, to_page=9)
                    except Exception:
                        pass
                merged.insert_pdf(tmp)
                tmp.close()

            temp_full_pdf = os.path.join(temp_dir, "finalgovt.pdf")
            merged.save(temp_full_pdf)
            merged.close()

            govt_pdf_df = extract_govt_pdf(temp_full_pdf)

        st.success(f"Govt DF created — {len(govt_pdf_df):,} rows")

    # -----------------------------------------------
    # 2. FETCH ZOHO
    # -----------------------------------------------
    with st.spinner("Fetching Zoho data..."):
        brands = fetch_all_brands()
        zoho_df = prepare_zoho(brands)
    st.success(f"Zoho DF created — {len(zoho_df):,} rows")

    # -----------------------------------------------
    # 3. LOAD TM-PILOT
    # -----------------------------------------------
    with st.spinner("Loading TM-Pilot Excel..."):
        tmpilot_df = prepare_tmpilot(tmpilot_file)
    st.success(f"TM-Pilot DF created — {len(tmpilot_df):,} rows")

    # -----------------------------------------------
    # 4. FIND RECORDS TM-PILOT MISSED
    # -----------------------------------------------
    if len(tmpilot_df) >= len(govt_pdf_df):
        missing = pd.DataFrame()
        st.success("No missing values in TM-Pilot")
    else:
        missing = govt_pdf_df[~govt_pdf_df["appno"].isin(tmpilot_df["appno"])]

    if not missing.empty:
        st.warning(f"TM-Pilot missed {len(missing)} records")
        st.dataframe(missing.head(50))

        missing_buf = BytesIO()
        with pd.ExcelWriter(missing_buf, engine="xlsxwriter") as writer:
            missing.to_excel(writer, index=False, sheet_name="Missing")
            worksheet = writer.sheets["Missing"]
            header_fmt = writer.book.add_format({"bold": True})
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

    # -----------------------------------------------
    # 5. BUILD THE COMPARISON SET
    # -----------------------------------------------
    page_lookup = (
        govt_pdf_df[["appno", "page_no"]]
        .dropna(subset=["appno"])
        .drop_duplicates(subset="appno", keep="first")
    )
    tmpilot = tmpilot_df.merge(page_lookup, on="appno", how="left")

    if not missing.empty:
        concatenated = pd.concat([tmpilot, missing], ignore_index=True)
    else:
        concatenated = tmpilot.copy()

    concatenated[["norm_core", "norm_full"]] = concatenated["tmAppliedFor"].apply(
        lambda x: pd.Series(normalise(x))
    )
    concatenated = concatenated[concatenated["norm_core"] != ""]
    concatenated["class"] = concatenated["class"].apply(clean_class)

    # -----------------------------------------------
    # 6. JOURNAL DATE (for the output filename)
    # -----------------------------------------------
    journal_date = datetime.now().strftime("%d-%m-%Y")
    try:
        jd_raw = concatenated["JournalDate"].dropna().iloc[0]
        journal_date = pd.to_datetime(jd_raw, format="%d/%m/%Y").strftime("%d-%m-%Y")
    except Exception:
        pass
    st.session_state["journal_date"] = journal_date

    # # -----------------------------------------------
    # # 7. RUN BOTH ENGINES
    # # -----------------------------------------------
    # with st.spinner("Running both engines for comparison..."):
    #     cmp_buf, matches_df, dropped = run_similarity(concatenated, zoho_df) #run_comparison(concatenated, zoho_df)

    # st.session_state["matches_df"] = matches_df
    # st.session_state["cmp_bytes"] = cmp_buf.getvalue()
    # st.session_state["dropped_count"] = len(dropped)

    # st.success(f"New engine flagged {len(matches_df):,} matches")

        # -----------------------------------------------
    # 7. RUN SIMILARITY ENGINE
    # -----------------------------------------------
    with st.spinner("Running similarity engine..."):
        matches_df = run_similarity(concatenated, zoho_df)

    st.session_state["matches_df"] = matches_df
    st.success(f"Flagged {len(matches_df):,} matches")


# ---------------------------------------------------
# RESULTS SECTION
# ---------------------------------------------------
st.markdown("---")
st.subheader("Output")

matches_df = st.session_state.get("matches_df")

if matches_df is not None and not matches_df.empty:

    # left, right = st.columns(2)
    # with left:
    #     st.write("**Match types**")
    #     st.write(matches_df["match_type"].value_counts())
    # with right:
    #     st.write("**Old engine comparison**")
    #     st.write(f"{st.session_state['dropped_count']:,} matches no longer flagged")
    st.write("**Match types**")
    st.write(matches_df["match_type"].value_counts())
    # st.dataframe(matches_df.head(50))

    # main report
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        matches_df.to_excel(writer, index=False, sheet_name="Matches")
        worksheet = writer.sheets["Matches"]
        header_format = writer.book.add_format({"bold": True})
        for col_idx, col_name in enumerate(matches_df.columns):
            worksheet.write(0, col_idx, col_name, header_format)
        worksheet.freeze_panes(1, 0)
    buf.seek(0)

    jd = st.session_state.get("journal_date") or datetime.now().strftime("%d-%m-%Y")

    # dl1, dl2 = st.columns(2)
    # with dl1:
    #     st.download_button(
    #         "Download Similarity Report",
    #         data=buf,
    #         file_name=f"Similarity_Report_for_Jnrl_{jd}.xlsx",
    #         mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    #     )
    # with dl2:
    #     if st.session_state.get("cmp_bytes"):
    #         st.download_button(
    #             "Download OLD vs NEW comparison",
    #             data=st.session_state["cmp_bytes"],
    #             file_name=f"Engine_Comparison_{jd}.xlsx",
    #             mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    #         )
    st.download_button(
        "Download Similarity Report",
        data=buf,
        file_name=f"Similarity_Report_for_Jnrl_{jd}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
else:
    st.info("Upload files and click Start Processing to begin.")
