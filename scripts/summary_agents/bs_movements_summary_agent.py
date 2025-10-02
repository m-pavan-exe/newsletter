# """Balance-sheet movements summarisation agent.

# Builds executive-ready summaries for each SOURCE line (and nested MRL / customer
# lines) in the formatted BS aggregation structure.  Uses the shared vLLM client
# `call_vllm_completion_or_chat` to obtain the narrative.
# """
from __future__ import annotations

from typing import List, Dict, Any
import json
import re
from llm_client.vllm_client import call_vllm_completion_or_chat





def format_customer_section(label: str, customers: List[Dict[str, Any]]) -> str:
    """Formats positive or negative customer sections with index hierarchy."""
    if not customers:
        return f"No customer is {label.lower()} influencing."
    lines = []
    count = len(customers)
    heading = f"Top {label} Influencing Customer{'s' if count > 1 else ''}:"
    for idx, c in enumerate(customers[:2], 1):
        lines.append(
            f"  {idx}.Name: {c['CUSTOMER_NAME']} (CIF: {c['CUSTOMER_ID']})\n"
            f"   - Category: {c.get('MRL_DESCRIPTION', '-')}\n"
            f"   - Todays Balance: {c['BUSINESS_DAY']}\n"
            f"   - Previous Day Balance: {c['PREVIOUS_DAY']}\n"
            f"   - {next((k.replace('_', ' ') for k in ['Increase_Amount', 'Decrease_Amount'] if k in c), 'N/A')} : {c.get('Increase_Amount', c.get('Decrease_Amount', 'N/A'))}\n"
        )
    return heading + "\n" + "\n".join(lines)

def summarize_bs_movements(
    agg_json_formatted: List[Dict[str, Any]],
    api_base: str,
    model_name: str,
) -> dict:
    """Generate only the top-level executive summary for all management lines.
    Returns a dict with 'TOP_LEVEL_SUMMARY'.
    """
    # Group management lines by type
    balance_sheet_items = []
    income_statement_items = []
    
    for tl_ctx in agg_json_formatted:
        desc = tl_ctx.get("MGT_LINE_DESCRIPTION", "")
        item = (
            f"Balance Sheet Section - {desc}\n"
            f"  • Todays Balance      : {tl_ctx['BUSINESS_DAY']}\n"
            f"  • Previous Day Balance: {tl_ctx['PREVIOUS_DAY']}\n"
            f"  • {next((k.replace('_', ' ') for k in ['Increase_Amount', 'Decrease_Amount'] if k in tl_ctx), 'N/A')} : {tl_ctx.get('Increase_Amount', tl_ctx.get('Decrease_Amount', 'N/A'))}\n"

        )
        # Categorize based on description
        if any(term in desc.upper() for term in ['ASSET', 'LIABILITY']):
            balance_sheet_items.append(item)
        elif any(term in desc.upper() for term in ['INCOME', 'EXPENSE']):
            income_statement_items.append(item)
    
    # Generate Balance Sheet summary (Assets & Liabilities)
    if balance_sheet_items:
        bs_prompt = (
            "You are a senior banker preparing a professional, concise summary of the company's financial position for the C-suite.\n\n"
            "DATA:\n"
            + "\n".join(balance_sheet_items)
            + (
                "\n\nTASK:\n"
                "- Clearly compare Total Assets and Total Liabilities, including absolute balances and any changes.\n"
                "- Highlight significant movements or trends, such as increases or decreases in Assets or Liabilities, and discuss their potential implications for the company's financial health.\n"
                "- Avoid vague statements or generic phrases; base all comments strictly on the provided data.\n"
                "- Write 2-4 sentences in a professional tone, without any headers, bullet points, or section labels.\n"
                "- Output only the summary text.\n"
            )
        )
        # print(bs_prompt)
        # print("\n\n")
        bs_summary_raw = call_vllm_completion_or_chat(bs_prompt, api_base, model_name, thinking=False).strip()
        bs_summary = re.sub(r"(?is)<think>.*?</think>", "", bs_summary_raw).strip()
        # print(bs_summary)
    # Generate Income Statement summary (Income & Expenses)
    if income_statement_items:
        is_prompt = (
            "You are a senior banker preparing a professional, concise summary of the company's operating performance for the C-suite.\n\n"
            "DATA:\n"
            + "\n".join(income_statement_items)
            + (
                "\n\nTASK:\n"
                "- Clearly compare Total Income and Total Expenses, including absolute balances and any changes.\n"
                "- Highlight significant movements or trends, such as increases or decreases in Income or Expenses, and discuss their potential implications for the company's financial health.\n"
                "- Avoid vague statements or generic phrases; base all comments strictly on the provided data.\n"
                "- Write 2-4 sentences in a professional tone, without any headers, bullet points, or section labels.\n"
                "- Output only the summary text.\n"
            )
        )
        is_summary_raw = call_vllm_completion_or_chat(is_prompt, api_base, model_name, thinking=False).strip()
        is_summary = re.sub(r"(?is)<think>.*?</think>", "", is_summary_raw).strip()
        # print(is_prompt)
        # print("\n\n")
        # print(is_summary)
    # CRISP, BRIEF INTRODUCTION
    intro_prompt = (
        "You are a senior banking analyst preparing a daily balance sheet and income statement briefing for C-suite executives.\n\n"
        "TASK:\n"
        "- Write a single, concise, professionally worded introductory paragraph for the report.\n"
        "- Each time you generate this introduction, craft it with unique phrasing, varied sentence structure, and fresh word choice, so no two intros sound identical.\n"
        "- Keep it limited to 2 sentences.\n"
        "- Your tone must remain professional, polished, and suitable for busy executives who require clarity and brevity.\n"
        "- Output only the introductory paragraph without any headers, bullet points, or section labels.\n"
        "- No data is provided to you so only write a generic introduction.\n"
    )

    introduction_raw = call_vllm_completion_or_chat(intro_prompt, api_base, model_name, thinking=False).strip()
    introduction = re.sub(r"(?is)<think>.*?</think>", "", introduction_raw)

    # Attach summaries to each SIGNIFICANT_SOURCE
    for tl in agg_json_formatted:
        # Management lines: customer-level summaries
        if tl.get("MGT_LINE_DESCRIPTION") in ["Total Assets", "Total Liability"]:
            for src_idx, src in enumerate(tl.get("SIGNIFICANT_SOURCES", []), 1):
                src_movement = (
                    f"   - Name: {src['SOURCE_MGT_LINE_DESC']}\n"
                    f"   - Todays Balance: {src['BUSINESS_DAY']}\n"
                    f"   - Previous Day Balance: {src['PREVIOUS_DAY']}\n"
                    f"   - {next((k.replace('_', ' ') for k in ['Increase_Amount', 'Decrease_Amount'] if k in src), 'N/A')} : {src.get('Increase_Amount', src.get('Decrease_Amount', 'N/A'))}\n"
                )
                pos_section = format_customer_section("Positively", src.get("POSITIVE_CUSTOMERS", []))
                neg_section = format_customer_section("Negatively", src.get("NEGATIVE_CUSTOMERS", []))

                src_prompt = (
                    "You are a CFO preparing a concise, insightful executive summary for the C-suite on balance sheet movement." +
                    "\n\nDATA:\n" + "Product:\n" + src_movement + "\n" + pos_section + "\n" + neg_section +
                    "\n\nTASK:\n"
                    "Using the above data, write a concise executive summary in 3-5 sentences with a professional tone. The summary must:\n"
                    f"- Clearly describe the overall movement of the product **{src['SOURCE_MGT_LINE_DESC']}**, including the todays balance, previous day balance, and absolute change.\n"
                    "- If positive influencing customers exist:\n"
                    "  - For each, include: customer name, ID, category, increase amount, previous day balance, and todays balance.\n"
                    "- If negative influencing customers exist:\n"
                    "  - For each, include: customer name, ID, category, decrease amount, previous day balance, and todays balance.\n"
                    "- If there are no positive or negative influencers, clearly state: “No significant positive or negative customer movements were observed.”\n"
                    "- Enclose the product name in double asterisks (**), but do this only once at its first mention in the summary; do not repeat or apply asterisks to any other words or subsequent mentions of the product.\n"
                    "- Do not enclose any other text in any formatting or symbols.\n"
                    "- Integrate balance ranges naturally by varying sentence structure; avoid repetitive 'from X to Y' phrasing.\n"
                    "- Avoid unnecessary repetition or generic statements.\n"
                    "- Output only the final summary text."
                )



                # print("___________________________________________")
                # print(src_prompt)
                src_summary_raw = call_vllm_completion_or_chat(src_prompt, api_base, model_name, thinking=False).strip()
                src_summary_clean = re.sub(r"(?is)<think>.*?</think>", "", src_summary_raw)
                src_summary = re.sub(r"(?i)^\W*(\*\*)?executive\s+summary(\*\*)?\W*[:\-–]?\s*", "", src_summary_clean).strip()
                # print(src_summary_raw)
                # print("___________________________________________")



                src["SUMMARY"] = src_summary
        # Expense management lines: sub-product (MRL) summaries
        elif tl.get("MGT_LINE_DESCRIPTION") in ['Total Income', 'Total Expense']:
            for src in tl.get("SIGNIFICANT_SOURCES", []):
                is_income = tl["MGT_LINE_DESCRIPTION"] == "Total Income"

                change_key = next((k for k in ['Increase_Amount', 'Decrease_Amount'] if k in src), None)
                change_label = change_key.replace('_', ' ') if change_key else "Change"
                change_amount = src.get(change_key, 'N/A')

                term = "Income" if is_income else "Expense"
                src_movement = (
                    f"   - Product Name: {src['SOURCE_MGT_LINE_DESC']}\n"
                    f"   - Today's {term}: {src['BUSINESS_DAY']}\n"
                    f"   - Previous Day's {term}: {src['PREVIOUS_DAY']}\n"
                    f"   - {change_label}: {change_amount}\n"
                    f"   - Month To Date (MTD) {term}: {src['SRC_MTD']}\n"
                )

                sig_mrls = src.get("SIGNIFICANT_MRLS", [])
                sig_mrl_lines = []

                if sig_mrls:
                    for mrl_idx, mrl in enumerate(sig_mrls[:2], 1):
                        sig_mrl_lines.append(
                            f"    {mrl_idx}. Name: {mrl['MRL_DESCRIPTION']}\n"
                            f"    - Month To Date (MTD) {term}: {mrl['MRL_MTD']}\n"
                        )
                    if len(sig_mrl_lines) == 1:
                        sig_story = (
                            f"Top {'Positively Influencing Category' if is_income else 'Significant Category driving the Total Expense'}:\n"
                            + sig_mrl_lines[0]
                        )
                    else:
                        sig_story = (
                            f"Top 2 {'Positively Influencing Categories' if is_income else 'Significant Categories driving the Total Expense'}:\n"
                            + "\n".join(sig_mrl_lines)
                        )
                else:
                    sig_story = (
                        f"No significant categories were observed."
                    )

                mrl_section = sig_story + "\n"

                src_prompt = (
                    f"You are an experienced CFO writing a short, professional financial summary for the C-suite about an {term} line under **{src['SOURCE_MGT_LINE_DESC']}** based on the data below.\n\n"
                    "DATA:\n" + src_movement + "\n" + mrl_section +
                    "\nTASK:\n"
                    f"- Clearly describe how today's {term} changed compared to previous day, including the amount of the change.\n"
                    "- Mention the Month To Date (MTD) Amount.\n"
                    "- If significant categories exist, weave them naturally into the explanation as contributors; otherwise, state there were no significant categories.\n"
                    "- Enclose the product name in double asterisks (**) only at its first mention; do not repeat **.\n"
                    "- Avoid repetitive or generic phrases; write naturally in 3-5 concise, insightful sentences.\n"
                    f"- Do NOT start with 'Today's {term}...'\n"
                    "- Use diverse phrasing to avoid repetition.\n"
                    "- Return only the final summary text."
                )

                # print("___________________________________________")
                # print(src_prompt)
                src_summary_raw = call_vllm_completion_or_chat(src_prompt, api_base, model_name, thinking=False).strip()
                src_summary_clean = re.sub(r"(?is)<think>.*?</think>", "", src_summary_raw)
                src_summary = re.sub(r"(?i)^\W*(\*\*)?executive\s+summary(\*\*)?\W*[:\-–]?\s*", "", src_summary_clean).strip()
                # print(src_summary)
                # print("___________________________________________")

                src["SUMMARY"] = src_summary

    return {
        "BS_SUMMARY": bs_summary,
        "IS_SUMMARY": is_summary,
        "INTRODUCTION": introduction,
    }, agg_json_formatted







