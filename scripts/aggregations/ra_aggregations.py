"""
Aggregation module for summarization framework.
Contains functions to aggregate overcharge and undercharge data for downstream summarization.
"""

import pandas as pd
from typing import Dict, Any, List
from utils import get_logger

# Use centralized logging configuration
logger = get_logger(__name__)

TOP_N_DEFAULT: int = 3  # Default number of SOC rows to return per leakage type


def _ensure_numeric(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """Coerce the specified *cols* to numeric dtype inplace and return *df* for chaining."""
    for col in cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _build_totals(df: pd.DataFrame) -> Dict[str, float]:
    """Return dictionary with *today* and *mtd* totals for provided dataframe."""
    return {
        "today": df["BUSINESS_DAY"].sum(),
        "mtd": df["MTD"].sum(),
    }


def _top_soc(df: pd.DataFrame, top_n: int) -> pd.DataFrame:
    """Return top *top_n* SOC-aggregated rows sorted by Business_Day, MTD."""
    soc_group_cols = [
        "BUSINESS_LINE_DESCRIPTION",
        "SBU_DESCRIPTION",
        "VISION_OUC_DESCRIPTION",
        "AO_NAME",
        "CHANNEL_TYPE_DESCRIPTION",
        "CUSTOMER_ID",
        "CUSTOMER_NAME",
        "CONTRACT_ID",
    ]
    aggregated = (
        df.groupby(soc_group_cols)[["BUSINESS_DAY", "MTD"]]
        .sum()
        .sort_values(by=["BUSINESS_DAY", "MTD"], ascending=False)
        .head(top_n)
        .reset_index()
    )
    return aggregated


def _format_agg_row(row: pd.Series, currency: str, leakage_type: str) -> str:
    """Return a formatted multi-line string describing *row* for *leakage_type*."""
    amount_label = "Overcharge" if leakage_type == "overcharge" else "Undercharge"
    return (
        f"{row['BUSINESS_LINE_DESCRIPTION']}\n"
        f"{amount_label} Amount\t\t: \tToday: {currency} {row['BUSINESS_DAY']:,} | "
        f"MTD: {currency} {row['MTD']:,}\n"
        f"Segment\t\t\t: \t{row['SBU_DESCRIPTION']}\n"
        f"Branch\t\t\t\t: \t{row['VISION_OUC_DESCRIPTION']}\n"
        f"Account Officer\t\t: \t{row['AO_NAME']}\n"
        f"Channel\t\t\t: \t{row['CHANNEL_TYPE_DESCRIPTION']}\n"
        f"Customer - Customer_Id\t: \t{row['CUSTOMER_NAME']} - {row['CUSTOMER_ID']}\n"
        f"Account\t\t\t: \t{row['CONTRACT_ID']}\n\n"
    )


def get_overcharge_and_undercharge_aggregations(df: pd.DataFrame, currency_type: str = "") -> Dict[str, Any]:
    """
    Aggregate overcharge and undercharge data by business line and segment (SBU),
    extracting the mode for branch, account officer, channel, customer, and account.
    Ensures numeric columns for aggregation.
    """
    try:
        logger.info("Starting aggregation of leakage data.")
        logger.debug(f"Input DataFrame shape: {df.shape}, columns: {df.columns.tolist()}")
        df = _ensure_numeric(df, ["BUSINESS_DAY", "MTD"])
        df = df[df.CCY_TYPE == "LCY"]
        df.loc[:, "BUSINESS_DAY"] = df["BUSINESS_DAY"].abs()
        df.loc[:, "MTD"] = df["MTD"].abs()
        logger.debug(f"Filtered LCY DataFrame shape: {df.shape}")

        # Split into leakage types
        leakage_map = {
            "overcharge": df[df["BAL_TYPE"] == 208],
            "undercharge": df[df["BAL_TYPE"] == 207],
        }

        result: Dict[str, Any] = {}

        for leakage_type, ldf in leakage_map.items():
            logger.debug("%s rows: %s", leakage_type.capitalize(), ldf.shape[0])

            totals = _build_totals(ldf)
            sbu_totals = (
                ldf.groupby("SBU_DESCRIPTION")[["BUSINESS_DAY", "MTD"]]
                .sum()
                .to_dict(orient="index")
            )

            top_soc_df = _top_soc(ldf, TOP_N_DEFAULT)
            agg_list = [
                _format_agg_row(row, currency_type, leakage_type)
                for _, row in top_soc_df.iterrows()
            ]
            agg_details = top_soc_df[["BUSINESS_DAY"]].to_dict(orient="records")

            result[leakage_type] = {
                "totals": totals,
                "sbu_totals": sbu_totals,   
                "agg_list": agg_list,
                "agg_details": agg_details,
                "top_n": TOP_N_DEFAULT,
            }

        logger.info("Aggregation complete.")
        return result
    except Exception as e:
        logger.exception("Error in aggregate_leakage: %s", e)
        raise
