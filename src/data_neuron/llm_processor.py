import openai
from .prompts.sql_query_prompt import sql_query_prompt
from .api.main import stream_neuron_api


def process_with_llm(query: str, context: dict) -> str:
    prompt = sql_query_prompt(query, context)
    system_prompt = "You are a helpful assistant that generates SQL queries based on natural language questions."
    stream_neuron_api(prompt, instruction_prompt=system_prompt)
