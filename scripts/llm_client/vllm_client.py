"""
LLM client abstraction for vLLM API.
This module allows easy replacement with other LLM providers if needed.
"""
import os
import requests
import random
import logging
from typing import List
import re
# VLLM API base and model name must be passed in by the caller (do not load config or env here)

def call_vllm_completion_or_chat(prompt: str, api_base: str, model_name: str, thinking: bool = False) -> str:
    """Call vLLM API for chat/completion with robust error handling.

    Parameters
    ----------
    prompt : str
        The user prompt to send to the model.
    api_base : str
        Base URL for the vLLM HTTP endpoint (e.g. http://localhost:8000).
    model_name : str
        Name of the model to query (e.g. 'qwen3-4B').
    thinking : bool, optional
        When True, include the `enable_thinking` flag in the payload so that the
        backend returns chain-of-thought reasoning (if supported by the model).
        Defaults to False.
    """
    headers = {"Content-Type": "application/json"}
    chat_url = f"{api_base}/v1/chat/completions"
    # Construct message list with Qwen control tag for thinking mode
    messages = [
        {"role": "system", "content": "/think" if thinking else "/no_think"},
        {"role": "user", "content": prompt}
    ]
    # Use fixed sampling parameters for more deterministic output
    rand_temp = 0.8
    rand_top_p = 0.9

    chat_payload = {
        "model": model_name,
        "messages": messages,
        "temperature": rand_temp,
        "top_p": rand_top_p,
        "top_k": 21,
        "repetition_penalty": 1.0,
        "n": 1
    }
    # Explicitly control chain-of-thought / "thinking" output; avoid backend defaulting to ON
    # The exact key recognised by Qwen3/vLLM is `enable_thinking`; adjust if your deployment uses a different flag.
    chat_payload["enable_thinking"] = thinking
    try:
        resp = requests.post(chat_url, json=chat_payload, headers=headers, timeout=60)
        if resp.status_code == 200:
            return resp.json()["choices"][0]["message"]["content"]
        elif resp.status_code != 404:
            logging.error(f"vLLM chat endpoint returned status {resp.status_code}: {resp.text}")
            resp.raise_for_status()
    except Exception as e:
        logging.error(f"Chat endpoint failed: {e}")
    return "[Error generating response]"

def concurrent_llm_calls(prompts: List[str], api_base: str, model_name: str, max_workers: int = 4) -> List[str]:
    """
    Run multiple LLM prompts concurrently for improved speed. Returns list of responses in order.
    api_base and model_name must be provided by the caller.
    """
    import concurrent.futures
    results = [None] * len(prompts)
    def worker(idx, prompt):
        try:
            response = call_vllm_completion_or_chat(prompt, api_base, model_name)
            response = re.sub(r"(?is)<think>.*?</think>", "", response).strip()
            results[idx] = response
        except Exception as e:
            logging.error(f"LLM call failed for prompt {idx}: {e}")
            results[idx] = "[Error generating response]"
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(worker, i, p) for i, p in enumerate(prompts)]
        concurrent.futures.wait(futures)
    return results
