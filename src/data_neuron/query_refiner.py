import json
from typing import Dict, Tuple, List
from .context_loader import load_context
from .api.main import call_neuron_api
from .db_operations.factory import DatabaseFactory
from .utils.print import print_info, print_prompt


class LLMQueryRefiner:
    def __init__(self):
        self.context = load_context()
        self.db = DatabaseFactory.get_database()

    def get_schema_info(self) -> str:
        schema_info = "Database Schema:\n"
        for table_name, table_info in self.context['tables'].items():
            schema_info += f"Table: {table_name}\n"
            for column in table_info['columns']:
                schema_info += f"  - {column['name']} ({column['type']})\n"
        return schema_info

    def get_sample_data(self) -> str:
        sample_data = "Sample Data:\n"
        for table_name in self.context['tables']:
            query = f"SELECT * FROM {table_name} LIMIT 5"
            result = self.db.execute_query(query)
            sample_data += f"Table: {table_name}\n"
            for row in result:
                sample_data += f"  {row}\n"
        return sample_data

    def refine_query(self, user_query: str) -> Tuple[str, List[str]]:
        schema_info = self.get_schema_info()
        sample_data = self.get_sample_data()

        prompt = f"""
        Given the following database schema and sample data:

        {schema_info}

        {sample_data}

        The user has asked the following question:
        "{user_query}"

        Please refine this query to align it with the database structure. Your task is to:
        1. Identify any terms that might correspond to table names, column names, or data values.
        2. Replace any ambiguous or colloquial terms with their corresponding database terms.
        3. Resolve any multi-word phrases that might represent a single entity in the database.
        4. Provide a list of changes made to the original query.

        Return your response in the following JSON format:
        {{
            "refined_query": "Your refined query here",
            "changes": [
                "Change 1",
                "Change 2",
                ...
            ]
        }}

        If no refinements are necessary, return the original query and an empty list of changes.

        Example:
        For the user query: "What did john doe buy last week?"
        A possible response might be:
        {{
            "refined_query": "What products were purchased by the user with first_name 'John' and last_name 'Doe' in the past seven days?",
            "changes": [
                "Replaced 'john doe' with 'first_name 'John' and last_name 'Doe'' to match the users table structure",
                "Replaced 'buy' with 'purchased' to align with likely column names",
                "Replaced 'last week' with 'in the past seven days' for more precise time range"
            ]
        }}

        Please provide your response in this JSON structure.
        """

        response = call_neuron_api(prompt)

        try:
            parsed_response = json.loads(response)
            refined_query = parsed_response['refined_query']
            changes = parsed_response['changes']
        except json.JSONDecodeError:
            print("Error: LLM response was not in valid JSON format.")
            return user_query, []
        except KeyError:
            print("Error: LLM response did not contain expected keys.")
            return user_query, []

        return refined_query, changes


def process_query(user_query: str) -> str:
    refiner = LLMQueryRefiner()
    refined_query, changes = refiner.refine_query(user_query)

    if changes:
        print("We've made the following adjustments to your query:")
        for change in changes:
            print_prompt(f"- {change}")

        print_info(f"\nRefined query: {refined_query}\n")
        if not user_confirms(changes):
            return "Query refinement cancelled."

    return f"Refined query: {refined_query}"


def user_confirms(changes: List[str]) -> bool:
    return input("Do you want to proceed with these changes? (yes/no): ").lower().strip() == 'yes'


# Example usage
if __name__ == "__main__":
    user_query = "What did john doe buy last week?"
    result = process_query(user_query)
    print(result)
