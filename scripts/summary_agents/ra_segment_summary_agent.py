"""
Summarization agent for generating segment-level executive summaries using LLMs.
"""
from typing import Dict, Any
import logging
from llm_client.vllm_client import concurrent_llm_calls

def fmt(num, width=11):
    try:
        return f"{num:>{width},.0f}" if num is not None else "0".rjust(width)
    except Exception as e:
        logging.error(f"Error in fmt: {e}")
        return str(num).rjust(width)

def agent_overall_summary(agg: Dict[str, Any], currency_type: str = "", vllm_api_base: str = None, vllm_model_name: str = None) -> Dict[str, Any]:
    """
    Generate a summary of overcharge and undercharge by segment (SBU).
    """
    try:
        if not agg or 'overcharge' not in agg or 'undercharge' not in agg:
            raise ValueError("Invalid aggregation data structure")
        overcharge_totals_today = agg['overcharge']['totals']['today']
        overcharge_totals_mtd = agg['overcharge']['totals']['mtd']
        undercharge_totals_today = agg['undercharge']['totals']['today']
        undercharge_totals_mtd = agg['undercharge']['totals']['mtd']
        overcharge_sbu_totals = agg['overcharge']['sbu_totals']
        undercharge_sbu_totals = agg['undercharge']['sbu_totals']
        max_overcharge_sbu = max(overcharge_sbu_totals.items(), key=lambda x: x[1]['BUSINESS_DAY']) if overcharge_sbu_totals else ("N/A", {"BUSINESS_DAY": 0})
        min_overcharge_sbu = min(overcharge_sbu_totals.items(), key=lambda x: x[1]['BUSINESS_DAY']) if overcharge_sbu_totals else ("N/A", {"BUSINESS_DAY": 0})
        max_overcharge_pct = (max_overcharge_sbu[1]['BUSINESS_DAY'] / overcharge_totals_today * 100) if overcharge_totals_today > 0 else 0
        min_overcharge_val = min_overcharge_sbu[1]['BUSINESS_DAY']
        min_overcharge_pct = (min_overcharge_val / overcharge_totals_today * 100) if overcharge_totals_today > 0 else 0
        max_undercharge_sbu = max(undercharge_sbu_totals.items(), key=lambda x: x[1]['BUSINESS_DAY']) if undercharge_sbu_totals else ("N/A", {"BUSINESS_DAY": 0})
        min_undercharge_sbu = min(undercharge_sbu_totals.items(), key=lambda x: x[1]['BUSINESS_DAY']) if undercharge_sbu_totals else ("N/A", {"BUSINESS_DAY": 0})
        max_undercharge_pct = (max_undercharge_sbu[1]['BUSINESS_DAY'] / undercharge_totals_today * 100) if undercharge_totals_today > 0 else 0
        min_undercharge_val = min_undercharge_sbu[1]['BUSINESS_DAY']
        min_undercharge_pct = (min_undercharge_val / undercharge_totals_today * 100) if undercharge_totals_today > 0 else 0
        if overcharge_totals_today == 0 and undercharge_totals_today == 0:
            executive_prompt = (
                "There are no overcharge or undercharge amounts recorded for today."
            )
            llm_summary = executive_prompt
        else:
            executive_prompt = (
                "Write a clear and concise executive summary in exactly two paragraphs based strictly on the overcharge and undercharge statistics provided below."
                "Paragraph 1:"
                "- Focus only on overcharge data."
                "- Mention today's total, the month-to-date (MTD) total, the segment with the highest overcharge (include segment name, value, and percentage of today’s total), and the segment with the lowest overcharge (same details)."
                "- Use only the numbers and facts provided."
                "- Do not include causes, comparisons, recommendations, opinions, or generalizations."
                "Paragraph 2:"
                "- Focus only on undercharge data."
                "- Include today’s total, MTD total, most undercharged segment (name, value, percentage), and least undercharged segment (same details)."
                "- Follow the same restrictions as above: no analysis, interpretation, or qualitative language."
                f"Overcharge Totals Today: {currency_type}{fmt(overcharge_totals_today)}\n"
                f"Overcharge Totals MTD: {currency_type}{fmt(overcharge_totals_mtd)}\n"
                f"Most Overcharged Segment: {max_overcharge_sbu[0]} ({fmt(max_overcharge_sbu[1]['BUSINESS_DAY'])}/{fmt(overcharge_totals_today)} = {max_overcharge_pct:.2f}% of total today)\n"
                f"Least Overcharged Segment: {min_overcharge_sbu[0]} ({fmt(min_overcharge_val)}/{fmt(overcharge_totals_today)} = {min_overcharge_pct:.2f}% of total today)\n"
                f"Undercharge Totals Today: {currency_type}{fmt(undercharge_totals_today)}\n"
                f"Undercharge Totals MTD: {currency_type}{fmt(undercharge_totals_mtd)}\n"
                f"Most Undercharged Segment: {max_undercharge_sbu[0]} ({fmt(max_undercharge_sbu[1]['BUSINESS_DAY'])}/{fmt(undercharge_totals_today)} = {max_undercharge_pct:.2f}% of total today)\n"
                f"Least Undercharged Segment: {min_undercharge_sbu[0]} ({fmt(min_undercharge_val)}/{fmt(undercharge_totals_today)} = {min_undercharge_pct:.2f}% of total today)\n"
            )
            llm_summary = concurrent_llm_calls([executive_prompt], vllm_api_base, vllm_model_name)[0]
            # llm_summary = llm_summary.replace("\n\n", "\n")

        summary = {
            "overcharge_totals_today": currency_type+fmt(overcharge_totals_today),
            "overcharge_totals_mtd": currency_type+fmt(overcharge_totals_mtd),
            "undercharge_totals_today": currency_type+fmt(undercharge_totals_today),
            "undercharge_totals_mtd": currency_type+fmt(undercharge_totals_mtd),
            "segments_summary": llm_summary
        }
        return summary
    except Exception as e:
        logging.error(f"Error in agent_overall_summary: {e}")
        return {"segments_summary": f"Error: {str(e)}"}
