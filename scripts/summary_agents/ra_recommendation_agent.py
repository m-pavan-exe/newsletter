"""
Recommendation agent for generating context-aware recommendations for business line summaries.
"""
from typing import List
from llm_client.vllm_client import concurrent_llm_calls
import logging
import re

def clean_recommendation(text):
    try:
        if not text:
            return text
            
        # Remove markdown formatting (**text** -> text)
        text = re.sub(r'\*\*([^*]+)\*\*', r'\1', text)
        
        # Remove any leading whitespace/newlines, 'Recommendation', colons/periods, etc.
        text = re.sub(r'^[\s\*]*(Recommendation)[\s\*]*[:\\.]?[\s\*]*(\n+)?', '', text, flags=re.IGNORECASE)
        
        # Remove any remaining markdown characters
        text = re.sub(r'[\*_`#]', '', text)
        
        return text.strip()
    except Exception as e:
        logging.error(f"Error in clean_recommendation: {e}")
        return text

def generate_recommendations(summary_list: List[str], recommendation_list: List[str], 
                           vllm_api_base: str = None, vllm_model_name: str = None) -> List[str]:
    """
    Generate relevant and varied recommendations for each business line summary using LLM.
    
    Args:
        summary_list: List of business line summaries
        recommendation_list: List of possible recommendations to consider
        vllm_api_base: Base URL for the LLM API
        vllm_model_name: Name of the LLM model to use
        
    Returns:
        List of generated recommendations, one for each summary
    """
    if not summary_list or not recommendation_list:
        logging.warning("Empty summary list or recommendation list provided")
        return [""] * len(summary_list) if summary_list else []
    
    try:
        # Create prompts with all recommendations as context
        prompts = []
        context = "\n- ".join([""] + recommendation_list) if recommendation_list else "No specific recommendations available."
        
        # for summary in summary_list:
        #     prompt = (
        #         "Analyze the business summary and relevant context below. Generate specific, actionable recommendations.\n"
        #         "- Be concrete and specific\n"
        #         "- Start with a strong action verb\n"
        #         "- Keep it 1-2 sentences maximum\n"
        #         "- Focus on strategic, high-impact suggestions\n"
        #         "- Avoid generic advice and platitudes\n\n"
        #         f"BUSINESS SUMMARY:\n{summary}\n\n"
        #         f"POSSIBLE RECOMMENDATION AREAS:\n{context}\n\n"
        #         "RECOMMENDATION (start with verb, be specific):"
        #     )
        #     prompts.append(prompt)
        
        import random
        
        for summary in summary_list:
            # Select 2 random recommendation areas
            selected_areas = random.sample(recommendation_list, min(2, len(recommendation_list)))
            areas_context = "\n- ".join([""] + selected_areas)
            
            prompt = (
                "Generate a specific, actionable recommendation based on the following areas. "
                "Be direct and start with a verb. Keep it to 1-2 sentences.\n\n"
                f"FOCUS ON THESE AREAS ONLY:\n{areas_context}\n\n"
                "RECOMMENDATION:"
            )
            prompts.append(prompt)



        
        # Generate recommendations using LLM
        recs = concurrent_llm_calls(prompts, vllm_api_base, vllm_model_name)
        return [clean_recommendation(r) for r in recs]
        
    except Exception as e:
        logging.error(f"Error generating recommendations: {str(e)}")
        # Return empty recommendations in case of error
        return [""] * len(summary_list)
