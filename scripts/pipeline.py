"""
Pipeline orchestrator for the AI Summarization Modular Framework.
Connects data ingestion, aggregation, summarization from summaryagent, and output modules.
"""
import logging
import os
import json
from typing import Optional
from aggregations.ra_aggregations import get_overcharge_and_undercharge_aggregations
from aggregations.bs_aggregations import build_bs_nested_hierarchy, format_nested_amounts
from summary_agents.bs_movements_summary_agent import summarize_bs_movements
from summary_agents.ra_segment_summary_agent import agent_overall_summary
from summary_agents.ra_business_line_summary_agent import agent_business_line_summary
from summary_agents.ra_recommendation_agent import generate_recommendations
from export_helpers.ra_files_exporter import export_ra_ai_summary_to_pdf
from export_helpers.bs_files_exporter import export_bs_summary_to_docx_v4, export_bs_summary_to_html_v1, convert_docx_to_pdf
from db_access.db_writer import insert_insight_summary
from db_access.db_loader import fetch_dataframe, fetch_book_ccy,execute_custom_query, get_data_for_summary
from db_access.sql_queries import GET_CATALOG_REPORT2, GET_CATALOG_REPORT1
from summary_agents.bs_performance_summary_agent import generate_summary
from datetime import datetime
import pandas as pd
from utils import get_logger

# Initialize logger
logger = get_logger(__name__)


def run_bs_summary_pipeline(engine, country: str, le_book: str, business_date: Optional[str], config: dict,template_path: str, output_dir: str) -> Optional[dict]:
    """
    Orchestrates the BS summary pipeline. 
    
    Args:
        engine: SQLAlchemy engine
        country: Country code (e.g., 'KE')
        le_book: Ledger book code (e.g., '01')
        business_date: Business date (e.g., '2024-01-01')
        config: Configuration dictionary
        template_path: Path to the template file
        output_dir: Directory for output files
        
    Returns:
        dict: Results of the pipeline or None if failed
    """
    logger.info(f"Starting BS summary pipeline for country: {country}, le_book: {le_book}")
    logger.debug(f"Pipeline configuration: {json.dumps(config, indent=2)}")
    business_date = datetime.now().strftime("%Y-%m-%d")
    currency_type = fetch_book_ccy(engine, country)
    src_exclude_lines_bs = [
    "Cash",
    "CBK Accounts/CRR",
    "Fixed Assets",
    "Interbank Lending",
    "Interest Receivable",
    "Loan Loss Provision (Pool+Spe+Gen)",
    "Nostro Accounts",
    "Other Assets FTP",
    "Other Assets non-FTP",
    "Other Loans & Advances",
    "Capital / AFS Reserve",
    "Inter Sol Balances non-FTP",
    "Interest Payable",
    "Other Liabilities FTP",
    "Other Liabilities non-FTP"
    ]
    src_exclude_lines_is = [
    "CBK Accounts/CRR",
    "Interbank Lending",
    "Nostro Accounts",
    "Other Loans & Advances",
    "Allocated from HO",
    "Depreciation & Amortization",
    "Office Running Expenses",
    "Rental Charges",
    "Other Fees & Commissions",
    "Other Income",
    ]
    
    try:
        logger.info("Fetching data from database")
        df_bsc = execute_custom_query(engine, GET_CATALOG_REPORT1)
        df_isc = execute_custom_query(engine, GET_CATALOG_REPORT2)
        logger.debug(f"ALC query result shape: {df_bsc.shape}")
        logger.debug(f"IEC query result shape: {df_isc.shape}")

        # Data preprocessing
        df_bs = df_bsc.copy()
        df_bs['BUSINESS_DAY'] = df_bs['BUSINESS_DAY'].astype(float).round(0).astype(int)
        df_bs['PREVIOUS_DAY'] = df_bs['PREVIOUS_DAY'].astype(float).round(0).astype(int)
        df_is = df_isc.copy()
        df_is['BUSINESS_DAY'] = df_is['BUSINESS_DAY'].astype(float).round(0).astype(int)
        df_is['PREVIOUS_DAY'] = df_is['PREVIOUS_DAY'].astype(float).round(0).astype(int)
        df_is['MTD'] = df_is['MTD'].astype(float).round(0).astype(int)
        df = pd.concat([df_bs, df_is], ignore_index=True)
        logger.info(f"Combined dataframe shape: {df.shape}")
    except Exception as e:
        logger.error(f"Error in data preprocessing: {str(e)}", exc_info=True)
        raise
    # ---------------------- Thresholds ----------------------
    # source_threshold =0.5   # Source level percentage change threshold

    # ---------------------- Aggregations ----------------------
    
    logger.info("Building nested hierarchy")
    agg_json = build_bs_nested_hierarchy(
        df,
        exclude_lines=(src_exclude_lines_bs + src_exclude_lines_is),
    )
    logger.debug(f"Aggregation hierarchy structure: {json.dumps(agg_json, indent=2)}")

    logger.info("Formatting nested amounts")
    agg_json_formatted = format_nested_amounts(agg_json, currency_type)
    logger.debug(f"Formatted aggregation structure: {json.dumps(agg_json_formatted, indent=2)}")

    logger.info("Generating BS movements summary")
    summaries_dict, source_level_summaries = summarize_bs_movements(
        agg_json_formatted,
        api_base=config["VLLM_API_BASE"],
    model_name=config["VLLM_MODEL_NAME"],
    )
    
    docx_path = export_bs_summary_to_docx_v4(template_path, output_dir, summaries_dict, source_level_summaries)
    pdf_path = convert_docx_to_pdf(docx_path)



    summary_html = export_bs_summary_to_html_v1(summaries_dict, source_level_summaries)
    insert_insight_summary(engine, country, le_book, business_date, summary_html, pdf_path, 'bs_ai_insight_summary')
    return None

def run_ra_summary_pipeline(
    country: str,
    le_book: str,
    business_date: Optional[str],
    template_path: str,
    output_dir: str,
    engine=None,
    config=None
) -> dict:
    """
    Orchestrates the AI summary pipeline, passing vLLM API base and model name from config to all LLM agent calls.
    """
    # business_date = datetime.now().strftime("%Y-%m-%d")

    logger.info(f"Starting run_summary_pipeline with country={country}, le_book={le_book}, business_date={business_date}")
    # Step 2: Data ingestion , country, le_book, business_date='KE','01','2025-05-30'
    df = get_data_for_summary(engine, country, le_book,business_date)
    logger.debug(f"DataFrame shape after loading: {df.shape}")
    if df.empty:
        logger.error("No data found for the provided filters.")
        raise ValueError("No data found for the provided filters.")
    # Step 3: Aggregation
    currency_type = fetch_book_ccy(engine, country)
    logger.debug(f"Fetched currency_type: {currency_type}")
    agg = get_overcharge_and_undercharge_aggregations(df, currency_type)
    logger.debug(f"Aggregation result: {agg}")
    # Step 4: Summarization
    vllm_api_base = config["VLLM_API_BASE"]
    vllm_model_name = config["VLLM_MODEL_NAME"]
    overall_summary = agent_overall_summary(agg, currency_type, vllm_api_base, vllm_model_name)
    logger.debug(f"Overall summary: {overall_summary}")
    overcharge_header, overcharge_agg_list, overcharge_summaries = agent_business_line_summary(agg, "overcharge", vllm_api_base, vllm_model_name)
    undercharge_header, undercharge_agg_list, undercharge_summaries = agent_business_line_summary(agg, "undercharge", vllm_api_base, vllm_model_name)
    logger.debug(f"Overcharge BL summary: header={overcharge_header}, aggs={overcharge_agg_list}, summary={overcharge_summaries}")
    logger.debug(f"Undercharge BL summary: header={undercharge_header}, aggs={undercharge_agg_list}, summary={undercharge_summaries}")
    # Step 5: Recommendations (context-aware)
    recommendation_list_undercharge_100_pct = [
    "Missing charge configuration in the system.",
    "Incorrect rate setup.",
    "Charge trigger condition not met or wrongly coded.",
    "Exemptions applied without authorization.",
    "Transaction volume not mapped to tiered pricing logic.",
    "Product wrongly mapped to non-chargeable category.",
    "Limit-based waivers not revoked after usage exceeded.",
    "Changes to pricing not synced across systems."
     ]
    recommendation_list_undercharge_partial= [
    "Incorrect rate or slab applied",
    "Tiered Pricing mismatch",
    "FX Rate Variance",
    "Delayed pricing update",
    "Manual Intervention or Override",
    "Missing Parameters in charge formula",
    "Partial Reversal",
    "Wrong unit of measure applied",
    "Rounded-Off or truncated values",
    ]

    recommendation_list_overcharge = [
    "Duplicate charge posting",
    "Higher-than-configured rate applied",
    "Charges applied despite waiver or exemption",
    "Outdated or obsolete configuration used",
    "Wrong unit of measure applied",
    "Manual charge entry without proper authorization",
    "Double charges due to system retry or timeout"
     ]

    overcharge_recommendations = generate_recommendations(overcharge_summaries, recommendation_list_overcharge, vllm_api_base, vllm_model_name)
    undercharge_recommendations = generate_recommendations(undercharge_summaries, recommendation_list_undercharge_partial, vllm_api_base, vllm_model_name)
    logger.debug(f"Overcharge BL recommendations: {overcharge_recommendations}")
    logger.debug(f"Undercharge BL recommendations: {undercharge_recommendations}")
    # Step 6: Assemble summary
    summary_json = {
        "overcharge_totals_today": overall_summary['overcharge_totals_today'],
        "overcharge_totals_mtd": overall_summary['overcharge_totals_mtd'],
        "undercharge_totals_today": overall_summary['undercharge_totals_today'],
        "undercharge_totals_mtd": overall_summary['undercharge_totals_mtd'],
        "segments_summary": overall_summary['segments_summary'],
        "overcharge_business_line_header": overcharge_header,
        "overcharge_business_line_aggs": overcharge_agg_list,
        "overcharge_business_line_summary": overcharge_summaries,
        "overcharge_business_line_recommendations": overcharge_recommendations,
        "undercharge_business_line_header": undercharge_header,
        "undercharge_business_line_aggs": undercharge_agg_list,
        "undercharge_business_line_summary": undercharge_summaries,
        "undercharge_business_line_recommendations": undercharge_recommendations
    }
    formatted_date = datetime.today().strftime("%d-%B-%Y")
    
    summary_content = (
        f"<style>.tableAi {{border-collapse: collapse; width: 100%; margin-bottom: 15px;}} "
        ".tableAi td {padding: 8px; border: 1px solid #ddd; font-size: 14px;}"
        ".tableAi tr:nth-child(even) {background-color: #f8f9fa;}"
        ".tableAi tr:hover {background-color: #e9f7fe;}"
        ".tableAi td:first-child {font-weight: bold; width: 30%; color: #2c3e50;}"
        ".tableAi td:last-child {width: 70%;}</style>"
        f"<strong>Date:</strong> {formatted_date}\n\n"
        f"<strong>Leakage Summary Report</strong>\n\n"
        f"<strong>Total Overcharge Amount</strong>\t\t: \tToday: {summary_json['overcharge_totals_today']} | MTD: {summary_json['overcharge_totals_mtd']}\n"
        f"<strong>Total Undercharge Amount</strong>\t\t: \tToday: {summary_json['undercharge_totals_today']} | MTD: {summary_json['undercharge_totals_mtd']}\n"
        f"<strong>Summary</strong>:\n{summary_json['segments_summary']}\n\n"
        # f"<strong>SOC Lines</strong>\n\n"
        f"<strong>Overcharged SOC Lines</strong>\n\n{summary_json['overcharge_business_line_header']}\n" +
        "".join([
            f"{idx}. <strong>"+ aggs.split('\n',1)[0]+"</strong>\n"+
            '<table class="tableAi">\n' + 
            '\n'.join(
                # Modified TD styling removed since it's now in CSS
                f'  <tr>\n    <td>{line.split(":",1)[0].strip()}</td>\n    <td>{line.split(":",1)[1].strip() if ":" in line else ""}</td>\n  </tr>'
                for line in aggs.split('\n')[1:] if line.strip()
            ) + '</table>' +
            "<strong>Summary</strong>:\n"+summary+"\n<strong>Recommendation</strong>:\n"+recs+"\n\n" 
            for idx, (aggs,summary, recs) in enumerate(zip(
                summary_json['overcharge_business_line_aggs'],
                summary_json['overcharge_business_line_summary'],
                summary_json['overcharge_business_line_recommendations']
            ), 1)
        ]) + 
        f"<strong>Undercharged SOC Lines</strong>\n\n{summary_json['undercharge_business_line_header']}\n" +
        "".join([
            f"{idx}. <strong>"+ aggs.split('\n',1)[0]+"</strong>\n"+
            '<table class="tableAi">\n' + 
            '\n'.join(
                f'  <tr>\n    <td>{line.split(":",1)[0].strip()}</td>\n    <td>{line.split(":",1)[1].strip() if ":" in line else ""}</td>\n  </tr>'
                for line in aggs.split('\n')[1:] if line.strip()
            ) + '</table>' +
            "<strong>Summary</strong>:\n"+summary+"\n<strong>Recommendation</strong>:\n"+recs+"\n\n" 
            for idx, (aggs, summary, recs) in enumerate(zip(
                summary_json['undercharge_business_line_aggs'],
                summary_json['undercharge_business_line_summary'],
                summary_json['undercharge_business_line_recommendations']
            ), 1)
        ])
    )  

    logger.info(f"Summary JSON assembled. Keys: {list(summary_json.keys())}")
    # Step 7: Export results (Word/PDF)
#%%
    run_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    word_output_path = os.path.join(output_dir, f"ra_ai_summary_{run_time}.docx")
    logger.info(f"Exporting summary to Word/PDF at: {word_output_path}")
    export_ra_ai_summary_to_pdf(summary_json, template_path, word_output_path)
 #%%
    # Step 8: Insert summary into DB
    pdf_file_name = f"ra_ai_summary_{run_time}.pdf"
    logger.info(f"Inserting summary into DB with file name: {pdf_file_name}")
    insert_insight_summary(engine, country, le_book, business_date, summary_content, pdf_file_name, 'ra_ai_insight_summary')
    logger.info("Pipeline completed successfully in run_summary_pipeline.")
    return summary_json
