"""
Agent for generating performance vs target summaries.
"""
import logging
import pandas as pd
import numpy as np
from typing import Dict, Any, Union
from llm_client.vllm_client import call_vllm_completion_or_chat

def format_balance(value: Union[float, int, None]) -> str:
    """Format balance to millions or billions with appropriate suffix."""
    if pd.isna(value):
        return "N/A"
    if value == 0:
        return "0"
        
    abs_val = abs(value)
    sign = "" if value >= 0 else "-"
    
    if abs_val >= 1_000_000_000_000:  # Trillions
        return f"{sign}{abs_val/1_000_000_000_000:,.1f}T"
    elif abs_val >= 1_000_000_000:  # Billions
        return f"{sign}{abs_val/1_000_000_000:,.1f}B"
    elif abs_val >= 1_000_000:  # Millions
        return f"{sign}{abs_val/1_000_000:,.1f}M"
    elif abs_val >= 1_000:  # Thousands
        return f"{sign}{abs_val/1_000:,.1f}K"
    return f"{sign}{abs_val:,.0f}"

def generate_summary(
    f_df: pd.DataFrame,
    customer_balance_changes_df: pd.DataFrame,
    summary_type: str,
    api_base: str,
    model_name: str,
    currency: str
) -> str:
    """
    Generate a performance vs target summary using vLLM.
    
    Args:
        df: DataFrame containing the performance data
        mgt_line_list: List of MGT_LINE values to include in the summary
        summary_column: Column containing the pre-formatted highlights
        api_base: Base URL for the vLLM API
        model_name: Name of the model to use
        currency: Currency symbol to use in the summary
        
    Returns:
        str: Generated performance summary
    """
    try:
        # Filter and clean the data
        if summary_type == 'DTD':
            col_type=['BUSINESS_DAY','PREVIOUS_DAY']
            pct_col='BUSINESS_DAY_PERCENTAGE_CHANGE'

        elif summary_type == 'YTD':
            col_type=['YTD','YTD_TARGET']
            pct_col='YTD_VARIANCE'


        currency = currency+' '
        # Prepare customer insights data if available
        customer_insights = {}
        if summary_type == 'DTD' and customer_balance_changes_df is not None and not customer_balance_changes_df.empty:
            for _, cust_row in customer_balance_changes_df.iterrows():
                mgt_line = cust_row['MGT_LINE']
                if mgt_line not in customer_insights:
                    customer_insights[mgt_line] = []
                customer_insights[mgt_line].append({
                    'name': cust_row.get('CUSTOMER_NAME', 'Unknown'),
                    'id': cust_row.get('CUSTOMER_ID', 'N/A'),
                    'ao': cust_row.get('ACCOUNT_OFFICER', 'N/A'),
                    'ao_name': cust_row.get('AO_NAME', 'N/A'),
                    'previous': cust_row['PREVIOUS_DAY'],
                    'current': cust_row['BUSINESS_DAY'],
                    'change': cust_row['CHANGE'],
                    'pct_change': cust_row['PERCENT_CHANGE']
                })

        # Prepare data for LLM with growth/decline calculations
        formatted_data = []
        for _, row in f_df.iterrows():
            item = row['MGT_LINE_DESCRIPTION']
            current = row[col_type[0]]
            target_prev = row[col_type[1]]
            pct_change = row[pct_col]
            change_amt = current - target_prev
            mgt_line = row['MGT_LINE']
            
            if summary_type == 'YTD':
                data_point = {
                    "item": item,
                    "current": currency + format_balance(current),
                    "target": currency + format_balance(target_prev),
                    "variance": currency + format_balance(change_amt),
                    "variance_pct": f"{pct_change}%",
                }
            elif summary_type == 'DTD':
                # Prepare customer insights for this item if available
                customer_impacts = []
                if mgt_line in customer_insights and customer_insights[mgt_line]:
                    # Get top 3 customer impacts by absolute change
                    top_customers = sorted(
                        customer_insights[mgt_line], 
                        key=lambda x: abs(x['change']), 
                        reverse=True
                    )[:3]
                    
                    for cust in top_customers:
                        customer_impacts.append({
                            "customer_name": cust['name'],
                            "customer_id": cust['id'],
                            "ao": cust['ao'],
                            "ao_name": cust['ao_name'],
                            "previous_balance": f"{currency}{format_balance(cust['previous'])}",
                            "current_balance": f"{currency}{format_balance(cust['current'])}",
                            "change": f"{currency}{format_balance(cust['change'])}",
                            "pct_change": f"{abs(cust['pct_change']):.1f}%"
                        })
                
                data_point = {
                    "item": item,
                    "current": currency + format_balance(current),
                    "previous": currency + format_balance(target_prev),
                    "change": currency + format_balance(change_amt),
                    "change_pct": f"{pct_change}%",
                    "customer_insights": customer_impacts if customer_impacts else 
                                         [{"message": "No single customer accounts for more than 50% of the balance reduction in this category"}]
                }
            
            # Convert to markdown for better readability
            md_lines = ["```json"]
            for key, value in data_point.items():
                md_lines.append(f"  {key}: {value}")
            md_lines.append("```")
            
            formatted_data.append("\n".join(md_lines))

        data_str = "\n\n".join(formatted_data)

        # Update format instructions to use the same keys
        if summary_type == 'YTD':
            format_instructions = f"""
            For each item's data in the JSON blocks, generate a concise one-line summary comparing actual performance against target:

            Format for YTD:
            - "{{item}} stands at {{current}} vs target {{target}}, a {{variance_pct}} {{increase synonym/decrease synonym}} ({{underperforming/outperforming}} by {{variance}})."

            Guidelines:
            1. Use the exact item name as provided in the data - do not modify or rephrase it
            2. Use "stands at" for assets/liabilities, "totals" for income/expenses
            3. For positive variance_pct:
            - For expenses: this is bad (underperforming)
            - For assets/income: this is good (outperforming)
            4. For negative variance_pct:
            - For expenses: this is good (outperforming)
            - For assets/income: this is bad (underperforming)
            5. Use different synonyms for positive and negative changes
            6. Use "outperforming" for good performance, "underperforming" for bad performance
            7. Always include both current and target values
            """
        elif summary_type == 'DTD':
            format_instructions = """
            For each item's data in the JSON blocks, generate a clear summary following this structure:
            
            Format for DTD:
            - "{item} {positive change/negative change} from {previous} to {current}, a {change_pct} {positive change synonym/negative change synonym} ({change} {rise/drop})."
            
            If customer_insights is present and contains customer data, include a bullet point below the main summary 
            showing the top customer impacts with their respective account officers. If customer_insights contains a message, include that message instead.
            
            Example with customer impacts:
            - "Loans decreased from 10.5B to 9.8B, a 6.7% drop (0.7B decrease)."
              * Key customer impacts: 
                1. Customer A (ID: 123) [AO: John Smith (JS123)]: Decreased from 1.2B to 0.8B (0.4B, 33.3%)
                2. Customer B (ID: 456) [AO: Jane Doe (JD456)]: Decreased from 0.9B to 0.6B (0.3B, 33.3%)
            
            Example with no significant customer impacts:
            - "Deposits increased from 15.2B to 15.8B, a 3.9% rise (0.6B increase)."
              * No single customer accounts for more than 50% of the balance change in this category
            
            Guidelines:
            1. Use the exact item name as provided in the data - do not modify or rephrase it
            2. Start with the item name followed by a strong action verb
            3. Use different synonyms for positive and negative changes
            4. Include both change_pct and change values
            5. For items with customer_insights:
               - If it contains customer data, list up to 3 largest customer decreases
               - Format: "1. {customer_name} (ID: {customer_id}) [AO: {ao_name} ({ao_id})]: Decreased from X to Y (Z, P%)"
               - Sort by largest absolute decrease first
               - If it contains a message, include that message as is
            6. Include account officer information in the format [AO: {ao_name} ({ao_id})] after each customer's ID
            7. Keep the output concise but informative
            """
        # Update the prompt
        prompt = f"""You are a senior financial analyst creating an executive summary of key balance sheet movements.

        Below are the data points in JSON format:

        {data_str}

        {format_instructions}

        Keep the output professional and concise. Do not include any additional text or headers."""
        # print(data_str)
        # Generate the summary using vLLM
        summary = call_vllm_completion_or_chat(prompt, api_base, model_name)
        return summary.strip()
        
    except Exception as e:
        logging.error(f"Error generating performance summary: {str(e)}", exc_info=True)
        return f"[Error generating performance summary: {str(e)}]"