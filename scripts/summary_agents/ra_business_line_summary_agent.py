"""
Summarization agent for generating business line summaries for overcharge and undercharge.
"""
from typing import Dict, Any, List, Tuple
import logging
from llm_client.vllm_client import concurrent_llm_calls

def agent_business_line_summary(agg: Dict[str, Any], charge_type: str = "overcharge", vllm_api_base: str = None, vllm_model_name: str = None) -> Tuple[str, List[str], List[str]]:
    """
    Generate a summary of overcharge or undercharge by business line.
    Returns tuple of (header, agg_list, summary_list)
    """
    try:
        if not agg or charge_type not in agg:
            raise ValueError("Invalid aggregation data structure")
        agg_list = agg[charge_type].get('agg_list', [])
        if not agg_list:
            return f"No {charge_type}d business lines are observed.\n", [], []
        prompts = []
        if len(agg_list) == 1:
            header = f"Only one {charge_type}d business line group is observed. Following is its summary:"
        elif len(agg_list) == 2:
            header = f"Only two {charge_type}d business line groups are observed. Following are their summaries, ranked based on today's {charge_type} amount:"
        else:
            header = f"Following are the top {len(agg_list)} business line groups with the most {charge_type}s, ranked based on today's {charge_type} amount:"
        for idx, aggregation in enumerate(agg_list, 1):
            prompt = (
                f"Write a concise, human-readable summary for a business executive as a single paragraph. "
                f"Summarize only the {charge_type} aggregation fields and values provided below, focusing first on Today and Month-to-Date (MTD) {charge_type} amounts, then mentioning other fields in sequence. "
                f"Use only the provided aggregation fields and values—do not infer, derive, or introduce any information not present. "
                f"Do NOT add recommendations, unrelated information, or repeat/re-list the aggregation details. "
                f"Avoid repeating the same entity (such as branch, account officer, channel, or customer/account) more than once. "
                f"Explicitly mention both the name (key) and value for each aggregation field in context. "
                f"Express all numeric values rounded to the nearest thousand or million for readability.\n"
                f"Aggregations:\n{aggregation}\n"
                f"Include the following meta data in the summary: This is the #{idx} ranked most {charge_type}d business line group for today.\n"
                "\nExecutive Summary:\n"
            )
            prompts.append(prompt)
        summary_list = concurrent_llm_calls(prompts, vllm_api_base, vllm_model_name) if prompts else []
        return header, agg_list, summary_list
    except Exception as e:
        logging.error(f"Error in agent_business_line_summary: {e}")
        return f"Error generating {charge_type} business line summaries", [], [f"Error: {str(e)}"]
