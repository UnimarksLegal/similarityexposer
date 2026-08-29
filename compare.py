"""
compare.py
----------
Runs the OLD engine and the NEW engine on the same data and writes one Excel
with three sheets so a human can verify nothing real was lost.

Do not skip this. You are switching the logic that decides whether a client
gets told about a conflicting mark. Someone has to eyeball what disappeared.

Usage: in app.py, after concatenated and zoho_df exist, replace the
similarity block with:

    from compare import run_comparison
    buf, new_df, dropped = run_comparison(concatenated, zoho_df)
    st.session_state["matches_df"] = new_df
    st.download_button("Download OLD vs NEW comparison", data=buf,
        file_name="engine_comparison.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
"""

from io import BytesIO
import pandas as pd
import numpy as np
from rapidfuzz import fuzz, process

from matching import run_similarity
from prepareeve import clean_brand   # the ORIGINAL normaliser, kept for baseline


def _old_norm(df, src_col):
    """Recreate the original norm column so the baseline is the real thing,
    not the new normalisation wearing the old scorer."""
    d = df.copy()
    d["norm_old"] = d[src_col].apply(clean_brand)
    return d[d["norm_old"] != ""]


def run_old_engine(concatenated, zoho_df, limit=4, thresh=85):
    """Your original loop and your original normaliser, unchanged."""
    con_old = _old_norm(concatenated, "tmAppliedFor")
    zoho_old = _old_norm(zoho_df, "zoho_tm")

    results = []
    for cls, con in con_old.groupby("class"):
        zb = zoho_old[zoho_old["zohoclass"] == cls]
        if zb.empty:
            continue
        choices = dict(zip(zb.index, zb["norm_old"]))
        for _, crow in con.iterrows():
            matches = process.extract(
                crow["norm_old"], choices,
                scorer=fuzz.token_set_ratio, limit=limit, score_cutoff=thresh,
            )
            for _, score, zidx in matches:
                if not isinstance(zidx, (int, np.integer)):
                    continue
                zz = zb.loc[zidx]
                if isinstance(zz, pd.DataFrame):
                    zz = zz.iloc[0]
                results.append({
                    "govt_app_no": crow["appno"],
                    "govt_brand": crow["tmAppliedFor"],
                    "zoho_brand": zz["zoho_tm"],
                    "govt_class": cls,
                    "Compared_govt_name": crow["norm_old"],
                    "Compared_zoho_name": zz["norm_old"],
                    "score": score,
                })
    return pd.DataFrame(results)


def run_comparison(concatenated, zoho_df):
    old = run_old_engine(concatenated, zoho_df)
    new = run_similarity(concatenated, zoho_df)

    def keyset(df):
        if df.empty:
            return set()
        return set(zip(df["govt_app_no"].astype(str),
                       df["Compared_zoho_name"].astype(str)))

    old_keys, new_keys = keyset(old), keyset(new)

    dropped = old[old.apply(
        lambda r: (str(r["govt_app_no"]), str(r["Compared_zoho_name"])) not in new_keys,
        axis=1)] if not old.empty else pd.DataFrame()

    added = new[new.apply(
        lambda r: (str(r["govt_app_no"]), str(r["Compared_zoho_name"])) not in old_keys,
        axis=1)] if not new.empty else pd.DataFrame()

    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
        new.to_excel(w, index=False, sheet_name="NEW_keep")
        dropped.to_excel(w, index=False, sheet_name="DROPPED_review_these")
        added.to_excel(w, index=False, sheet_name="NEWLY_FOUND")
        summary = pd.DataFrame({
            "metric": ["old total", "new total", "dropped by new", "newly found by new"],
            "count": [len(old), len(new), len(dropped), len(added)],
        })
        summary.to_excel(w, index=False, sheet_name="Summary")
        for sheet in w.sheets.values():
            sheet.freeze_panes(1, 0)
    buf.seek(0)

    print(f"OLD {len(old)} | NEW {len(new)} | dropped {len(dropped)} | newly found {len(added)}")
    return buf, new, dropped
