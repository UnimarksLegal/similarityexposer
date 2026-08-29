"""
diagnose.py
-----------
Run this ONCE on a journal that produces bad output.
It tells you WHICH stage inflated, so you stop guessing.

How to use:
  1. Drop this file next to app.py
  2. In app.py, add at the top:      from diagnose import report
  3. At the very end of app.py, add: report(govt_pdf_df, tmpilot_df, zoho_df, concatenated, matches_df)
  4. Run once, copy the printed block.
"""

import pandas as pd


def _dup_count(df, col):
    if df is None or col not in df.columns:
        return "n/a"
    return int(df[col].duplicated().sum())


def report(govt_pdf_df=None, tmpilot_df=None, zoho_df=None,
           concatenated=None, matches_df=None, raw_zoho_records=None):

    out = []
    add = out.append

    add("=" * 60)
    add("SIMILARITY EXPOSER DIAGNOSTIC")
    add("=" * 60)

    # ---------- Stage 1: Zoho side ----------
    add("\n[1] ZOHO (your client database)")
    if raw_zoho_records is not None:
        add(f"    raw records pulled from Creator : {len(raw_zoho_records):,}")
        if len(raw_zoho_records):
            sample = raw_zoho_records[0]
            add(f"    sample field names             : {sorted(sample.keys())[:12]}")
            add(f"    Current_Status sample value    : {repr(sample.get('Current_Status'))}")
            add(f"    Class sample value             : {repr(sample.get('Class'))}")
    if zoho_df is not None:
        add(f"    rows AFTER status filter       : {len(zoho_df):,}")
        add(f"    duplicate zoho_appno rows      : {_dup_count(zoho_df, 'zoho_appno')}")
        add(f"    distinct classes               : {zoho_df['zohoclass'].nunique()}")
        short = zoho_df[zoho_df['norm_tm'].str.len() < 4]
        add(f"    normalised names under 4 chars : {len(short):,}  <-- collision fuel")
        if len(short):
            add(f"    examples                       : {short['norm_tm'].head(10).tolist()}")
        add("    top 10 normalised names by frequency (duplicates = noise multiplier):")
        for name, cnt in zoho_df['norm_tm'].value_counts().head(10).items():
            add(f"        {cnt:>5}x  {name!r}")

    # ---------- Stage 2: Govt PDF side ----------
    add("\n[2] GOVT PDF EXTRACT")
    if govt_pdf_df is not None:
        add(f"    rows parsed                    : {len(govt_pdf_df):,}")
        add(f"    duplicate appno rows           : {_dup_count(govt_pdf_df, 'appno')}  <-- merge explosion risk")
        img = (govt_pdf_df['tmAppliedFor'] == 'Image').sum()
        add(f"    rows where brand fell back to 'Image' : {img:,} ({img / max(len(govt_pdf_df),1):.0%})")
        add("      (if this % jumped, the journal PDF layout changed)")

    # ---------- Stage 3: TM-Pilot side ----------
    add("\n[3] TM-PILOT EXCEL")
    if tmpilot_df is not None:
        add(f"    rows                           : {len(tmpilot_df):,}")
        add(f"    duplicate appno rows           : {_dup_count(tmpilot_df, 'appno')}")

    # ---------- Stage 4: the merged set actually compared ----------
    add("\n[4] CONCATENATED (what actually gets compared)")
    if concatenated is not None:
        add(f"    rows                           : {len(concatenated):,}")
        add(f"    duplicate appno rows           : {_dup_count(concatenated, 'appno')}")
        if tmpilot_df is not None and len(concatenated) > len(tmpilot_df) * 1.1:
            add("    *** ROW EXPLOSION: concatenated is much bigger than TM-Pilot.")
            add("    *** The merge on appno is duplicating rows. See fix #2.")
        short = concatenated[concatenated['norm_tmp'].str.len() < 4]
        add(f"    normalised names under 4 chars : {len(short):,}")
        if len(short):
            add(f"    examples                       : {short['norm_tmp'].head(10).tolist()}")

    # ---------- Stage 5: output ----------
    add("\n[5] MATCHES")
    if matches_df is not None and len(matches_df):
        add(f"    total flagged                  : {len(matches_df):,}")
        pair_dupes = matches_df.duplicated(
            subset=[c for c in ['govt_app_no', 'zoho_Application_no'] if c in matches_df.columns]
        ).sum()
        add(f"    duplicate (govt,zoho) pairs    : {pair_dupes:,}  <-- pure noise, dedupe these")
        add(f"    distinct govt applications     : {matches_df['govt_app_no'].nunique():,}")
        add(f"    matches per govt application   : {len(matches_df)/max(matches_df['govt_app_no'].nunique(),1):.1f}")
        add("    score distribution:")
        for lo, hi in [(85, 90), (90, 95), (95, 100), (100, 101)]:
            n = matches_df[(matches_df['score'] >= lo) & (matches_df['score'] < hi)].shape[0]
            add(f"        {lo}-{hi if hi <= 100 else 100}: {n:,}")
        add("    10 worst offenders (short names matching at 100):")
        junk = matches_df[matches_df['score'] >= 99].copy()
        if 'Compared_govt_name' in junk.columns:
            junk['len'] = junk['Compared_govt_name'].str.len()
            for _, r in junk.nsmallest(10, 'len').iterrows():
                add(f"        {r['Compared_govt_name']!r} <-> {r['Compared_zoho_name']!r}")

    add("\n" + "=" * 60)

    text = "\n".join(out)
    print(text)
    try:
        import streamlit as st
        st.code(text)
    except Exception:
        pass
    return text


def versions():
    """Print installed library versions. Compare against a machine where it still works."""
    import importlib
    for mod in ["streamlit", "pandas", "numpy", "rapidfuzz", "fitz", "requests", "openpyxl"]:
        try:
            m = importlib.import_module(mod)
            v = getattr(m, "__version__", getattr(m, "version", "unknown"))
            print(f"{mod:<12} {v}")
        except Exception as e:
            print(f"{mod:<12} NOT INSTALLED ({e})")
