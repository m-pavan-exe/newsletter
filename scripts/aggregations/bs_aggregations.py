"""Business-specific aggregation helpers for the Balance-Sheet (BS) module.

This file was re-generated after accidental deletion.  It now contains:

1. build_bs_nested_hierarchy – replicates the logic that used to live inline in
   `run_bs_summary_pipeline2` for producing the nested JSON structure
   (Top-level MGT → Source → MRL → top 3 customers).

2. format_scaled_amount – utility that formats a numeric amount with the
   correct thousands-scale (K, M, B, T) and prepends the currency symbol/code.

These helpers are imported by pipeline.py but can be reused elsewhere as well.
"""
from __future__ import annotations

from typing import List, Dict, Any

import pandas as pd
from collections import OrderedDict, defaultdict

# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------


def build_bs_nested_hierarchy(
    df: pd.DataFrame,
    # source_threshold: float = 0.5,
    exclude_lines: list[str] = [],
) -> list[dict]:
    """
    Enhanced BS aggregation for special MGT_LINEs and optimized output.

    1. Apply source_threshold for all MGT_LINEs.
    2. For {G031999, G032999}:
        - Find top 1 +ve and top 2 -ve customers per source (SRC),
        - Include MRL_DESCRIPTION, ACCOUNT_OFFICER, AO_NAME, BUSINESS_DAY, PREVIOUS_DAY, CUST_PCT_CHANGE, CUST_CHANGE.
    3. For {G034999, G035999}:
        - Only include significant MRL metadata and change stats.
    4. Output nested JSON.

    :param df: Input DataFrame
    :param source_threshold: Threshold for source percentage change
    :param mrl_threshold: Threshold for MRL percentage change
    :param exclude_lines: List of MGT_LINEs to exclude
    :return: Nested JSON structure
    """
    # 1. Pre-aggregation and thresholding
    grp_cols_top = [ "MGT_LINE_DESCRIPTION"]
    top_level_sums = (
        df.groupby(grp_cols_top, as_index=False)[["BUSINESS_DAY", "PREVIOUS_DAY","MTD"]].sum()
    )
    top_level_sums["TL_CHANGE"] = top_level_sums["BUSINESS_DAY"] - top_level_sums["PREVIOUS_DAY"]
    top_level_sums["TL_PCT_CHANGE"] = (top_level_sums["BUSINESS_DAY"] - top_level_sums["PREVIOUS_DAY"]) / top_level_sums["PREVIOUS_DAY"].replace({0: pd.NA}) * 100
    top_level_sums.rename({"MTD": "TL_MTD"}, axis=1, inplace=True)

    df = df[~df['SOURCE_MGT_LINE_DESC'].isin(exclude_lines)]
    grp_cols_src = ["MGT_LINE_DESCRIPTION","SOURCE_MGT_LINE","SOURCE_MGT_LINE_DESC"]
    source_sums = (
        df.groupby(grp_cols_src, as_index=False)[["BUSINESS_DAY", "PREVIOUS_DAY","MTD"]].sum()
    )
    source_sums["SRC_CHANGE"] = source_sums["BUSINESS_DAY"] - source_sums["PREVIOUS_DAY"]
    source_sums["SRC_PCT_CHANGE"] = (source_sums["BUSINESS_DAY"] - source_sums["PREVIOUS_DAY"]) / source_sums["PREVIOUS_DAY"].replace({0: pd.NA}) * 100
    source_sums.rename({"MTD": "SRC_MTD"}, axis=1, inplace=True)

    # Apply source_threshold to all MGT_LINEs
    # src_sig = source_sums[source_sums["SRC_PCT_CHANGE"].abs() >= source_threshold]

    # MRL sums for meta
    grp_cols_mrl = ["MGT_LINE_DESCRIPTION","SOURCE_MGT_LINE", "MRL_LINE"]
    mrl_sums = (
        df.groupby(grp_cols_mrl, as_index=False)[["BUSINESS_DAY", "PREVIOUS_DAY","MTD"]].sum()
    )
    mrl_sums["MRL_CHANGE"] = mrl_sums["BUSINESS_DAY"] - mrl_sums["PREVIOUS_DAY"]
    mrl_sums["MRL_PCT_CHANGE"] = (mrl_sums["BUSINESS_DAY"] - mrl_sums["PREVIOUS_DAY"]) / mrl_sums["PREVIOUS_DAY"].replace({0: pd.NA}) * 100
    mrl_sums.rename({"MTD": "MRL_MTD"}, axis=1, inplace=True)

    # Customer sums for special MGTs
    grp_cols_cust = [
        "MGT_LINE_DESCRIPTION",
        "SOURCE_MGT_LINE",
        "MRL_LINE",
        "MRL_DESCRIPTION",
        "CUSTOMER_ID",
        "CUSTOMER_NAME",
        "ACCOUNT_OFFICER",
        "AO_NAME",
    ]
    customer_sums = (
        df.groupby(grp_cols_cust, as_index=False)[["BUSINESS_DAY", "PREVIOUS_DAY"]].sum()
    )
    customer_sums["CUST_CHANGE"] = customer_sums["BUSINESS_DAY"] - customer_sums["PREVIOUS_DAY"]
    customer_sums["CUST_PCT_CHANGE"] = (customer_sums["BUSINESS_DAY"] - customer_sums["PREVIOUS_DAY"]) / customer_sums["PREVIOUS_DAY"].replace({0: pd.NA}) * 100

    # 2. Special handling for G031999/G032999: top +ve and -ve customers per source
    pos_neg_src_lookup = defaultdict(lambda: {"POS": [], "NEG": []})
    for rec in customer_sums.to_dict("records"):
        if rec["MGT_LINE_DESCRIPTION"] in ["Total Assets", "Total Liability"]:
            key = (rec["MGT_LINE_DESCRIPTION"], rec["SOURCE_MGT_LINE"])
            cust_trimmed = {k: rec[k] for k in ["MRL_DESCRIPTION","ACCOUNT_OFFICER","AO_NAME","BUSINESS_DAY","PREVIOUS_DAY","CUST_PCT_CHANGE","CUST_CHANGE","CUSTOMER_ID","CUSTOMER_NAME"] if k in rec}
            if rec["CUST_CHANGE"] >= 0:
                pos_neg_src_lookup[key]["POS"].append(cust_trimmed)
            else:
                pos_neg_src_lookup[key]["NEG"].append(cust_trimmed)
    # Reduce to top 1 POS, top 2 NEG
    for key, buckets in pos_neg_src_lookup.items():
        buckets["POS"] = sorted(buckets["POS"], key=lambda r: r.get("CUST_CHANGE", 0), reverse=True)[:1]
        buckets["NEG"] = sorted(buckets["NEG"], key=lambda r: r.get("CUST_CHANGE", 0))[:2]

    # Build MRL_DESCRIPTION lookup for (MGT_LINE, SOURCE_MGT_LINE, MRL_LINE)
    mrl_description_lookup = df.drop_duplicates(subset=["MGT_LINE_DESCRIPTION", "SOURCE_MGT_LINE", "MRL_LINE"]).set_index(["MGT_LINE_DESCRIPTION", "SOURCE_MGT_LINE", "MRL_LINE"])['MRL_DESCRIPTION'].to_dict()

    # 3. Build nested JSON
    nested_json = []
    # Precompute source counts for each MGT_LINE
    all_sources = source_sums.groupby("MGT_LINE_DESCRIPTION").size().to_dict()
    sig_sources = source_sums.groupby("MGT_LINE_DESCRIPTION").size().to_dict()

    for tl in top_level_sums.to_dict("records"):
        mgt_desc = tl["MGT_LINE_DESCRIPTION"]
        mgt_dict = OrderedDict(tl)
        sources_for_mgt = source_sums[source_sums["MGT_LINE_DESCRIPTION"] == mgt_desc]
        source_records = []
        for src in sources_for_mgt.to_dict("records"):
            # Remove MGT_LINE key from source
            src_dict = OrderedDict({k: v for k, v in src.items() if k != "MGT_LINE_DESCRIPTION"})
            key_src = (mgt_desc, src["SOURCE_MGT_LINE"])
            # G031999/G032999: add POSITIVE/NEGATIVE_CUSTOMERS at source level
            if mgt_desc in ["Total Assets", "Total Liability"]:
                buckets = pos_neg_src_lookup.get(key_src, {"POS": [], "NEG": []})
                src_dict["POSITIVE_CUSTOMERS"] = buckets["POS"]
                src_dict["NEGATIVE_CUSTOMERS"] = buckets["NEG"]
            # G034999/G035999: only significant MRL meta
            if mgt_desc in ["Total Expense", "Total Income"]:
                mrls = mrl_sums[(mrl_sums["MGT_LINE_DESCRIPTION"] == mgt_desc) & (mrl_sums["SOURCE_MGT_LINE"] == src["SOURCE_MGT_LINE"])]
                if not mrls.empty:
                    # Filter for MRLs with MRL_MTD > 0 before selecting top 2
                    positive_mrls = mrls[mrls["MRL_MTD"] > 0].copy()
                    if not positive_mrls.empty:
                        positive_mrls.loc[:,"MRL_DESCRIPTION"] = positive_mrls.apply(
                            lambda rec: mrl_description_lookup.get((mgt_desc, src["SOURCE_MGT_LINE"], rec["MRL_LINE"]), rec.get("MRL_DESCRIPTION", "-")),
                            axis=1
                        )
                        mrl_list_pos = positive_mrls.nlargest(2, "MRL_MTD").to_dict("records")
                        src_dict["SIGNIFICANT_MRLS"] = mrl_list_pos
                    else:
                        src_dict["SIGNIFICANT_MRLS"] = []
                else:
                    src_dict["SIGNIFICANT_MRLS"] = []
            source_records.append(dict(src_dict))
        # Sort SIGNIFICANT_MRLS by absolute value of MRL_CHANGE descending if present
        if mgt_desc in ["Total Assets", "Total Liability"]:
            # Sort SIGNIFICANT_SOURCES by absolute value of SRC_CHANGE descending
            source_records_sorted = sorted(
                source_records,
                key=lambda s: abs(s.get("SRC_CHANGE", 0)),
                reverse=True
            )
        else:
            source_records_sorted = sorted(
                source_records,
                key=lambda s: abs(s.get("SRC_MTD", 0)),
                reverse=True
            )
            for src in source_records_sorted:
                if "SIGNIFICANT_MRLS" in src:
                    src["SIGNIFICANT_MRLS"] = sorted(
                        src["SIGNIFICANT_MRLS"],
                        key=lambda m: abs(m.get("MRL_MTD", 0)),
                        reverse=True
                    )
        # Add counts to MGT_LINE level
        mgt_dict["SIGNIFICANT_SOURCES_COUNT"] = sig_sources.get(mgt_desc, 0)
        mgt_dict["SOURCES_COUNT"] = all_sources.get(mgt_desc, 0)
        mgt_dict["SIGNIFICANT_SOURCES"] = source_records_sorted
        nested_json.append(dict(mgt_dict))
    return nested_json



# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

_CURRENCY_SYMBOLS = {
    "USD": "$",
    "EUR": "€",
    "GBP": "£",
    "JPY": "¥",
    "INR": "₹",
    "CNY": "¥",
    "AUD": "A$",
    "CAD": "C$",
}


def format_scaled_amount(value: float | int, currency_type: str = "") -> str:
    """Return *value* formatted to nearest T/B/M/K with currency prefix.

    Examples: 1500000 → "$1.5M" (if currency_type="USD"),
              -1234    → "₹-1.23K" (if currency_type="INR").
    """
    if value is None:
        return "0"

    symbol = _CURRENCY_SYMBOLS.get(currency_type.upper(), f"{currency_type} ") if currency_type else ""

    abs_val = abs(value)
    scale = ""
    divisor = 1
    if abs_val >= 1e12:
        scale = "T"
        divisor = 1e12
    elif abs_val >= 1e9:
        scale = "B"
        divisor = 1e9
    elif abs_val >= 1e6:
        scale = "M"
        divisor = 1e6
    elif abs_val >= 1e3:
        scale = "K"
        divisor = 1e3

    scaled = value / divisor
    formatted = f"{scaled:,.2f}"
    sign = "-" if value < 0 else ""
    return f"{symbol}{sign}{formatted}{scale}"

import copy

# -------------------- Scaling helpers -------------------- #
#scale of changes drives the consistent scale for related keys)

def _get_scale_info(val: float | int | None) -> tuple[float, str]:
    if val is None:
        return 1, ""
    abs_val = abs(val)
    if abs_val < 1e3:
        return 1, ""
    scales = [(1e12, "T"), (1e9, "B"), (1e6, "M"), (1e3, "K")]
    for divisor, suffix in scales:
        if abs_val / divisor >= 10:
            return divisor, suffix
    return 1, ""

def _get_scale_info_fixed(val: float | int | None) -> tuple[float, str]:
    """Return divisor and scale suffix for a given absolute numeric value."""
    if val is None:
        return 1, ""
    abs_val = abs(val)
    if abs_val >= 1e12:
        return 1e12, "T"
    elif abs_val >= 1e9:
        return 1e9, "B"
    elif abs_val >= 1e6:
        return 1e6, "M"
    elif abs_val >= 1e3:
        return 1e3, "K"
    return 1, ""

def _format_fixed_scale(currency_type: str,val: float | int | None, divisor: float, suffix: str,decimal_places: int = 2) -> str:
    """Format *val* using provided *divisor* and scale *suffix*."""
    if val is None:
        return "0"
    symbol = _CURRENCY_SYMBOLS.get(currency_type.upper(), f"{currency_type} ") if currency_type else ""
    scaled = val / divisor
    formatted = f"{scaled:,.{decimal_places}f}"  # Added comma as thousands separator
    return f"{symbol}{formatted}{suffix}"

def _format_dict(d: dict, currency_type: str, amount_keys: list[str]):
    new_dict = {}
    change_key = None
    rel_divisor, rel_suffix = None, None
    
    # First: handle *_CHANGE key with relative scale
    for key in list(d.keys()):
        if key.endswith("_CHANGE") and not key.endswith("_PCT_CHANGE") and d[key] is not None:
            change_key = key
            change_value = d[key]
            direction = 'Decrease' if isinstance(change_value, (int, float)) and change_value < 0 else 'Increase'
            new_key = f"{direction}_Amount"
            
            rel_divisor, rel_suffix = _get_scale_info(change_value)
            new_dict[new_key] = _format_fixed_scale(
                currency_type,
                abs(change_value) if direction == 'Decrease' else change_value,
                rel_divisor,
                rel_suffix,
                decimal_places=0
            )
    
    # Handle *_PCT_CHANGE key
    for key in list(d.keys()):
        if key.endswith("_PCT_CHANGE") and d[key] is not None:
            val = d[key]
            direction = 'Decrease' if isinstance(val, (int, float)) and val < 0 else 'Increase'
            new_key = f"{direction}_Percentage"
            new_dict[new_key] = f"{abs(val):.2f}%"
    
    # Process amount keys: store both relative & fixed scales
    for k in ["BUSINESS_DAY", "PREVIOUS_DAY","SRC_MTD","TL_MTD"]:
        if k in d and not k.endswith("_PCT_CHANGE"):
            # Relative scale version: keep original key name
            if rel_divisor:
                new_dict[k] = _format_fixed_scale(currency_type, d[k], rel_divisor, rel_suffix,decimal_places=0)
            else:
                new_dict[k] = "N/A"  # Or optionally skip
            
            # Fixed scale version: add _FIX suffix
            fix_div, fix_suf = _get_scale_info_fixed(d[k])
            new_dict[f"{k}_FIX"] = _format_fixed_scale(currency_type, d[k], fix_div, fix_suf,decimal_places=2)
    
    # Copy other keys unchanged
    for key in list(d.keys()):
        if key not in new_dict and not key.endswith("_CHANGE") and not key.endswith("_PCT_CHANGE"):
            new_dict[key] = d[key]
    
    # Update original dictionary preserving order
    d.clear()
    d.update(new_dict)
    
    # Recursively format nested structures
    if "SIGNIFICANT_SOURCES" in d:
        for src in d["SIGNIFICANT_SOURCES"]:
            _format_dict(src, currency_type, amount_keys)
            if "SIGNIFICANT_MRLS" in src:
                for mrl in src["SIGNIFICANT_MRLS"]:
                    _format_dict(mrl, currency_type, amount_keys)
            if "POSITIVE_CUSTOMERS" in src:
                for cust in src["POSITIVE_CUSTOMERS"]:
                    _format_dict(cust, currency_type, amount_keys)
            if "NEGATIVE_CUSTOMERS" in src:
                for cust in src["NEGATIVE_CUSTOMERS"]:
                    _format_dict(cust, currency_type, amount_keys)



def format_nested_amounts(
    nested_json: list[dict],
    currency_type: str,
    amount_keys: tuple[str, ...] = ("BUSINESS_DAY", "PREVIOUS_DAY","SRC_MTD","TL_MTD","MRL_MTD"),
) -> list[dict]:
    """Recursively format BUSINESS_DAY/PREVIOUS_DAY at all levels of *nested_json*.

    The function creates a deep copy of the provided list, leaving the original
    data structure untouched. The top-level items are ordered as:
    1. Total Assets
    2. Total Liability 
    3. Total Income
    4. Total Expense
    Any other MGT_LINE_DESCRIPTION values will appear after these in their original order.
    
    Within each top-level category, significant sources are ordered as follows:
    - Total Assets/Income: 1. Loans, 2. Overdrafts, 3. Public Securities, 4. Government Securities, others, Other Assets last
    - Total Liability/Expense: 1. High Cost Deposits, 2. Low Cost Deposits, 3. Long Term Borrowings, 
                              4. Interbank Borrowings, others, Other Liabilities last
    """
    # Define the desired order for top-level items
    top_level_order = {
        "Total Assets": 0,
        "Total Liability": 1,
        "Total Income": 2,
        "Total Expense": 3
    }
    
    # Define the desired order for significant sources within each top-level category
    source_priority = {
        "Total Assets": ["Loans", "Overdrafts", "Public  Securities", "Government Securities"],
        "Total Income": ["Loans", "Overdrafts", "Public  Securities", "Government Securities"],
        "Total Liability": [ "Low Cost Deposits","High Cost Deposits", "Long Term Borrowings", "Interbank Borrowings"],
        "Total Expense": ["Low Cost Deposits","High Cost Deposits", "Long Term Borrowings", "Interbank Borrowings"]
    }
    
    # Sort the nested_json based on the top-level priority order
    def get_top_level_priority(item):
        return top_level_order.get(item.get("MGT_LINE_DESCRIPTION"), float('inf'))
    
    # Sort significant sources within each top-level category
    def sort_significant_sources(item):
        mgt_desc = item.get("MGT_LINE_DESCRIPTION")
        if mgt_desc in source_priority and "SIGNIFICANT_SOURCES" in item and item["SIGNIFICANT_SOURCES"]:
            priority_list = source_priority[mgt_desc]
            other_src = "Other Operating Expenses" if "Expense" in mgt_desc else ("Other Assets" if "Asset" in mgt_desc or "Income" in mgt_desc else "Other Liabilities")
            
            # Create a priority dictionary for O(1) lookups, with stripped whitespace for matching
            priority_dict = {src.strip(): idx for idx, src in enumerate(priority_list)}
            
            def get_source_priority(source):
                src_desc = source.get("SOURCE_MGT_LINE_DESC", "").strip()
                
                # First try exact match
                if src_desc in priority_dict:
                    return priority_dict[src_desc]
                    
                # Then try case-insensitive match
                src_desc_lower = src_desc.lower()
                for priority_src, idx in priority_dict.items():
                    if priority_src.lower() == src_desc_lower:
                        return idx
                        
                # If it's the "Other" category, put it at the end
                if src_desc == other_src.strip() or src_desc_lower == other_src.lower().strip():
                    return float('inf')
                    
                # Other items come after priority items but before "Other"
                return len(priority_list)
            
            # Create a copy of the sources list to avoid modifying during iteration
            sources = list(item["SIGNIFICANT_SOURCES"])
            
            # Sort the sources based on the priority
            sources.sort(key=get_source_priority)
            
            # Update the original list
            item["SIGNIFICANT_SOURCES"][:] = sources
        return item
    
    # Process the nested JSON
    processed_json = []
    for item in nested_json:
        # Sort significant sources within this top-level item
        item = sort_significant_sources(copy.deepcopy(item))
        processed_json.append(item)
    
    # Sort top-level items
    sorted_json = sorted(processed_json, key=get_top_level_priority)
    
    # Process and format the sorted items
    formatted: list[dict] = []
    for tl in sorted_json:
        _format_dict(tl, currency_type, amount_keys)
        formatted.append(tl)
    return formatted
