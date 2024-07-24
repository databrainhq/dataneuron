import openai
from .prompts.sql_query_prompt import sql_query_prompt


def process_with_llm(query: str, context: dict) -> str:
    prompt = sql_query_prompt(query, context)
    system_prompt = "You are a helpful assistant that generates SQL queries based on natural language questions."
    response = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt}
        ]
    )
    return response.choices[0].message['content'].strip()
