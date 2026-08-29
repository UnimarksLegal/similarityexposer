"""
matching.py
-----------
Replacement for the normalisation + similarity logic.

What changed and why:

1. NORMALISATION no longer destroys marks.
   Old clean_brand stripped single letters A-Z as whole words and ~90 generic
   phrases via one big regex. "SUN PHARMA" became "SUN". "A ONE FOODS" became
   "ONE". Once dozens of different marks all collapse to the same 3-letter
   stub, token_set_ratio scores them 100 against each other. That is where
   your junk matches come from.

   Now: generic words are removed token by token, and if removal would leave
   nothing useful, the original is kept instead of being blanked.

2. TWO STRINGS ARE KEPT per mark: core (generics stripped) and full (raw).
   Scoring uses both, so "SUN PHARMA" vs "SUN CARE" no longer looks identical.

3. GUARDS after candidate generation. token_set_ratio returns 100 whenever one
   token set is a subset of the other. That is deliberate for trademark watch
   work, but only meaningful when the shared token is distinctive. A shared
   token of "GOLD" or "SS" is not.

4. MATCH TYPE column so your team triages instead of reading 300 rows blind.
"""

import re
import pandas as pd
from rapidfuzz import fuzz, process
from rapidfuzz.distance import Levenshtein

# ---------------------------------------------------------------
# Generic / non-distinctive tokens. Removed only when other tokens
# survive. Deliberately does NOT include single letters.
# ---------------------------------------------------------------
GENERIC_TOKENS = {
    "PRIVATE", "PVT", "LIMITED", "LTD", "LLP", "COMPANY", "CO", "INC", "CORP",
    "GROUP", "ENTERPRISES", "ENTERPRISE", "INDUSTRIES", "TRADERS", "TRADING",
    "DEVICE", "LOGO", "LABEL", "MARK", "WITH", "THE", "AND", "OF", "IMAGE",
    "SOLUTIONS", "SERVICES", "SERVICE", "PRODUCTS", "PRODUCT", "UNIT",
    "TECHNOLOGIES", "TECHNOLOGY", "INTERNATIONAL", "GLOBAL", "INDIA", "INDIAN",
    "HINDI", "ENGLISH", "BRAND", "BRANDS",
}

# Descriptive / trade words. These STAY in the compared string, but a match
# that rests only on one of them is rejected as non-distinctive.
WEAK_TOKENS = {
    "FOODS", "FOOD", "PHARMA", "PHARMACEUTICAL", "PHARMACEUTICALS", "PHARMACY",
    "HEALTHCARE", "HEALTH", "CARE", "CARES", "CLINIC", "CLINICS", "HOSPITAL",
    "HOSPITALS", "ORGANIC", "ORGANICS", "AGRO", "MASALA", "GOLD", "GOLDS",
    "JEWELS", "JEWELLERS", "JEWELLERY", "STUDIO", "STUDIOS", "SCHOOL",
    "SPORTS", "CINEMAS", "CINEMA", "ENTERTAINMENT", "ENTERTAINMENTS",
    "CONSTRUCTIONS", "CONSTRUCTION", "PUBLICATIONS", "GRANULES", "VALUE",
    "VALUES", "SILVER", "PLUS", "SUPER", "NEW", "ROYAL", "STAR", "KING",
    "TECH", "BIO", "LIFE", "HOME", "CITY", "POWER", "PRIME", "SMART", "MAX",
    # trade descriptors seen in your own journal data
    "FASHION", "FASHIONS", "TRENDS", "TREND", "DESIGNS", "DESIGN", "CLOTHING",
    "APPAREL", "APPARELS", "GARMENTS", "INTERIORS", "INTERIOR", "SHIRTS",
    "DHOTHIES", "COTTON", "SILKS", "TEXTILES", "STAINLESS", "STEEL",
    "SCRUBBER", "QUALITY", "HEAVY", "PREMIUM", "GRAND", "MISCELLANEOUS",
    "NUTRITION", "FUTURE", "COFFEE", "CAFE", "PAINT", "PAINTS", "EDUTECH",
}

# Only true boilerplate is stripped. Weak words survive so that "GOLD PLUS"
# does not collapse into a 4-letter stub that collides with everything.
STOP_ALL = GENERIC_TOKENS | WEAK_TOKENS   # used ONLY for distinctiveness tests

# A mark whose every word is one of these has no word element to compare.
# "Device", "LOGO", "(LABEL)" are placeholders for an image, not brand names.
NON_MARK_TOKENS = {
    "DEVICE", "DEVICES", "LOGO", "LOGOS", "IMAGE", "IMAGES", "LABEL", "LABELS",
    "MARK", "MARKS", "WORDMARK", "OF", "THE", "AND", "WITH", "NULLS", "NULL",
}

_PAREN_RE = re.compile(r"\([^)]*\)")
_DEVICE_LEAD_RE = re.compile(r"^\s*(DEVICE\s+OF\b|[A-Z]\s+DEVICE\b)")
_NONALNUM_RE = re.compile(r"[^A-Z0-9]+")

# tune these two, they are the only knobs you should touch
MIN_CORE_LEN = 4          # ignore normalised names shorter than this
MIN_DISTINCTIVE_LEN = 5   # a subset match needs a shared token at least this long


def normalise(s):
    """Return (core, full). core has generics stripped, full does not."""
    if pd.isna(s):
        return "", ""
    raw = str(s).upper()

    # pure device marks carry no word to compare
    if _DEVICE_LEAD_RE.match(raw):
        return "", ""

    raw = _PAREN_RE.sub(" ", raw)
    raw = _NONALNUM_RE.sub(" ", raw)
    tokens = [t for t in raw.split() if t]
    if not tokens:
        return "", ""

    # Pure image marks: nothing to fuzzy-match against. Drop them entirely.
    if all(t in NON_MARK_TOKENS for t in tokens):
        return "", ""

    full = " ".join(tokens)

    core = [t for t in tokens if t not in GENERIC_TOKENS and len(t) > 1]
    if not core:
        # Everything was boilerplate or initials ("S S ENTERPRISES").
        # Keep the whole string. Collapsing to one word creates false EXACTs.
        core = tokens

    return " ".join(core), full


def _length_ratio(a, b):
    if not a or not b:
        return 0.0
    return min(len(a), len(b)) / max(len(a), len(b))


def _shared_distinctive(a, b):
    """Longest token present in both, excluding generic/weak words."""
    sa = {t for t in a.split() if t not in STOP_ALL}
    sb = {t for t in b.split() if t not in STOP_ALL}
    shared = sa & sb
    return max((len(t) for t in shared), default=0)


def _has_distinctive(s):
    return any(t not in STOP_ALL and len(t) >= MIN_DISTINCTIVE_LEN for t in s.split())


def _strong_form(core):
    """Core with descriptive trade words removed. "DEVITE CARE" -> "DEVITE".
    Falls back to the core when stripping would leave nothing."""
    kept = [t for t in core.split() if t not in WEAK_TOKENS]
    return " ".join(kept) if kept else core


def _evaluate(a, b):
    """Rule set applied to one pair of strings. Returns (type, score) or (None, None)."""
    if a == b:
        return "EXACT", 100

    # Short single-word marks: one character apart matters more than any ratio.
    if max(len(a), len(b)) <= 8 and " " not in a and " " not in b:
        dist = Levenshtein.distance(a, b)
        if dist <= 1:
            return "STRONG", 95
        if dist == 2 and min(len(a), len(b)) >= 5:
            return "FUZZY", 85

    sort_score = fuzz.token_sort_ratio(a, b)
    if sort_score >= 88 and _length_ratio(a, b) >= 0.65:
        return "STRONG", int(sort_score)

    set_score = fuzz.token_set_ratio(a, b)
    if set_score >= 92 and _shared_distinctive(a, b) >= MIN_DISTINCTIVE_LEN:
        return "CONTAINED", int(set_score)

    plain = fuzz.ratio(a, b)
    if plain >= 85 and _length_ratio(a, b) >= 0.75:
        return "FUZZY", int(plain)

    return None, None


def classify(g_core, g_full, z_core, z_full):
    """Return (match_type, score), or (None, None) to drop the pair."""
    if not g_core or not z_core:
        return None, None

    if len(g_core) < MIN_CORE_LEN or len(z_core) < MIN_CORE_LEN:
        # Short cores are usually stripping debris ("RLD" left from a longer
        # mark). Genuine short marks exist (MRF, TVS), so accept only when
        # nothing was stripped on either side and they match exactly.
        untouched = (g_core == g_full) and (z_core == z_full)
        if untouched and g_core == z_core:
            return "EXACT", 100
        return None, None

    # Multi-word marks where neither side has a distinctive word
    # ("S S ENTERPRISES" vs "S K ENTERPRISES"). Only an exact hit counts.
    multi = " " in g_core and " " in z_core
    if multi and not _has_distinctive(g_core) and not _has_distinctive(z_core):
        return ("EXACT", 100) if g_full == z_full else (None, None)

    # Pass 1: full cores.
    mt, score = _evaluate(g_core, z_core)
    if mt:
        return mt, score

    # Pass 2: strip descriptive trade words and retry. Catches
    # "P-DEVIT" vs "DEVITE CARE" and "ZAGNUS" vs "AGNUS PHARMACEUTICALS",
    # which pass 1 misses because the trade word drags the ratio down.
    gs, zs = _strong_form(g_core), _strong_form(z_core)
    if (gs, zs) == (g_core, z_core):
        return None, None
    if len(gs) < MIN_DISTINCTIVE_LEN or len(zs) < MIN_DISTINCTIVE_LEN:
        return None, None

    mt, score = _evaluate(gs, zs)
    if mt and mt != "CONTAINED":
        return mt, score
    # containment on stripped forms is too loose to trust
    return None, None


def run_similarity(concatenated: pd.DataFrame, zoho_df: pd.DataFrame,
                   candidate_limit: int = 12,
                   candidate_cutoff: int = 70) -> pd.DataFrame:
    """
    concatenated needs: norm_core, norm_full, class, appno, tmAppliedFor,
                        buisnessName, page_no, JournalDate, goodsAndSerice
    zoho_df needs:      norm_core, norm_full, zohoclass, zoho_tm, zoho_appno, ...
    """
    results = []

    for cls, con in concatenated.groupby("class"):
        pool = zoho_df[zoho_df["zohoclass"] == cls]
        if pool.empty:
            continue

        # Compare against DISTINCT names only. Your database holds 71 copies of
        # NIPPON PAINT; without this they consume every candidate slot and
        # genuine conflicts never surface.
        uniq = pool.drop_duplicates(subset="norm_core").copy()
        uniq["norm_strong"] = uniq["norm_core"].map(_strong_form)
        choices_core = dict(zip(uniq.index, uniq["norm_core"]))
        choices_strong = dict(zip(uniq.index, uniq["norm_strong"]))

        for _, crow in con.iterrows():
            g_core, g_full = crow["norm_core"], crow["norm_full"]
            if len(g_core) < 2:
                continue   # classify() decides on short marks, not this loop

            # Two prefilter passes. The first finds ordinary near-matches.
            # The second strips descriptive trade words from BOTH sides first,
            # because "DEVIT" vs "DEVITE CARE" scores only 62 as-is and would
            # never reach classify(). This is where P-DEVIT / DEVITE CARE and
            # ZAGNUS / AGNUS PHARMACEUTICALS were being lost.
            seen = {}
            for src, key in ((choices_core, g_core),
                             (choices_strong, _strong_form(g_core))):
                for _, _pre, zidx in process.extract(
                    key, src,
                    scorer=fuzz.token_set_ratio,
                    limit=candidate_limit,
                    score_cutoff=candidate_cutoff,
                ):
                    seen[zidx] = True

            for zidx in seen:
                zz = uniq.loc[zidx]
                if isinstance(zz, pd.DataFrame):
                    zz = zz.iloc[0]

                match_type, score = classify(
                    g_core, g_full, zz["norm_core"], zz["norm_full"]
                )
                if match_type is None:
                    continue

                # every one of our records carrying this same name
                sibs = pool[pool["norm_core"] == zz["norm_core"]]
                clients = sorted({str(c) for c in sibs.get("our_client", pd.Series(dtype=str)).dropna()})
                appnos = [str(a) for a in sibs["zoho_appno"].dropna().unique()]

                results.append({
                    "match_type": match_type,
                    "score": score,
                    "govt_app_no": crow["appno"],
                    "govt_brand": crow["tmAppliedFor"],
                    "zoho_brand": zz["zoho_tm"],
                    "govt_class": cls,
                    "zoho_class": zz["zohoclass"],
                    "our_records_with_this_name": len(sibs),
                    "zoho_client": ", ".join(clients[:5]) + ("  +more" if len(clients) > 5 else ""),
                    "zoho_Application_no": ", ".join(appnos[:5]) + ("  +more" if len(appnos) > 5 else ""),
                    "zoho_company1": zz.get("zoho_cmp1"),
                    "zoho_company2": zz.get("zoho_cmp2"),
                    "Compared_govt_name": g_core,
                    "Compared_zoho_name": zz["norm_core"],
                    "Govt_company_name": crow.get("buisnessName"),
                    "Journal_Date": crow.get("JournalDate"),
                    "Govt_Goods": crow.get("goodsAndSerice"),
                    "Zoho_goods": zz.get("zoho_goods"),
                    "Govt_pdf_pageno": crow.get("page_no"),
                })

    if not results:
        return pd.DataFrame()

    out = pd.DataFrame(results)
    # one row per (journal application, distinct name of ours)
    out = out.drop_duplicates(subset=["govt_app_no", "Compared_zoho_name"])

    order = {"EXACT": 0, "STRONG": 1, "CONTAINED": 2, "FUZZY": 3}
    out["_o"] = out["match_type"].map(order)
    out = out.sort_values(["_o", "score"], ascending=[True, False]).drop(columns="_o")
    return out.reset_index(drop=True)
