import json
from typing import Dict, Tuple, List
from .context_loader import load_context
from .api.main import call_neuron_api
from .db_operations.factory import DatabaseFactory
from .db_operations.database_helpers import DatabaseHelper, top_few_records
from .utils.print import print_info, print_prompt, print_warning


class LLMQueryRefiner:
    def __init__(self, context):
        self.context = context
        self.db = DatabaseFactory.get_database()
        self.explanation = ""
        self.db_helper = DatabaseHelper(
            self.context['database'], self.db)

    def get_schema_info(self) -> str:
        schema_info = "Database Schema:\n"
        for full_table_name, table_info in self.context['tables'].items():
            schema_info += f"Table: {full_table_name}\n"
            for column in table_info['columns']:
                schema_info += f"  - {column['name']} ({column['type']})\n"
        return schema_info

    def get_sample_data(self) -> str:
        sample_data = "Sample Data:\n"
        for full_table_name in self.context['tables']:
            query = top_few_records(
                self.db_helper, "*", full_table_name, limit=5)
            result = self.db.execute_query(query)
            sample_data += f"Table: {full_table_name}\n"
            for row in result:
                sample_data += f"  {row}\n"
        return sample_data

    def validate_and_refine_entities(self, entities: List[Dict]) -> Tuple[bool, List[Dict], List[Dict]]:
        refined_entities = []
        invalid_entities = []
        for entity in entities:
            query = top_few_records(
                self.db_helper,
                entity['column'],
                entity['table'],
                entity['potential_value']
            )
            results = self.db.execute_query(query)
            if results:
                matches = [row[0] for row in results]
                refined_entities.append({
                    'table': entity['table'],
                    'column': entity['column'],
                    'original_value': entity['potential_value'],
                    'matches': matches
                })
            else:
                invalid_entities.append(entity)

        is_valid = len(invalid_entities) == 0
        return is_valid, refined_entities, invalid_entities

    def refine_query(self, user_query: str) -> Tuple[str, List[str], List[Dict], List[Dict], bool]:
        schema_info = self.get_schema_info()
        sample_data = self.get_sample_data()

        prompt = f"""
        Given the following database schema and sample data:

        {schema_info}

        {sample_data}

        The user has asked the following question:
        "{user_query}"

        Please analyze if this query can be answered using the given database schema. If it can be answered, refine the query to align it with the database structure. If it cannot be answered, explain why.

        Your task is to:
        1. Determine if the query can be answered using the given schema.
        2. If it can be answered:
           a. Identify any terms that might correspond to schema names, table names, column names, or data values.
           b. Replace any ambiguous or colloquial terms with their corresponding database terms.
           c. Resolve any multi-word phrases that might represent a single entity in the database.
           d. Provide a list of changes made to the original query.
           e. Identify specific entities (column values) that need to be validated against the database.
           f. Use phrases like "containing", for potential matches.
           g. If there are no specific column values to validate, return an empty array for entities.
           h. Always use fully qualified table names (schema.table) in your refined query and explanations.
        3. If it cannot be answered, provide a clear explanation why.

        Return your response in the following JSON format:
        {{
            "can_be_answered": true/false,
            "explanation": "Your explanation here",
            "refined_query": "Your refined question here (if applicable)",
            "changes": [
                "Change 1",
                "Change 2",
                ...
            ],
            "entities": [
                {{
                    "table": "schema.table_name",
                    "column": "column_name",
                    "potential_value": "value to check"
                }},
                ...
            ]
        }}

        Please provide your response in this JSON structure. Strictly json no other words.
        """

        response = call_neuron_api(prompt)

        try:
            parsed_response = json.loads(response)
            can_be_answered = parsed_response['can_be_answered']
            self.explanation = parsed_response['explanation']

            if not can_be_answered:
                return user_query, [], [], [], False

            refined_query = parsed_response['refined_query']
            changes = parsed_response['changes']
            entities = parsed_response.get('entities', [])
        except (json.JSONDecodeError, KeyError):
            print("Error: Invalid response from LLM.")
            return user_query, [], [], [], False

        if entities:
            is_valid, refined_entities, invalid_entities = self.validate_and_refine_entities(
                entities)
            if refined_entities:
                refined_query = self.further_refine_query(
                    refined_query, refined_entities)
        else:
            is_valid, refined_entities, invalid_entities = True, [], []

        return refined_query, changes, refined_entities, invalid_entities, True

    def further_refine_query(self, query: str, refined_entities: List[Dict]) -> str:
        for entity in refined_entities:
            matches_len = len(entity['matches'])
            if matches_len == 1:
                query = query.replace(
                    f"containing '{entity['original_value']}'",
                    f"equal to '{entity['matches'][0]}'"
                )
            elif matches_len > 1 and matches_len < 10:
                match_list = "', '".join(entity['matches'])
                query = query.replace(
                    f"containing '{entity['original_value']}'",
                    f"in ('{match_list}')"
                )
        return query


def process_query(user_query: str, context: Dict) -> Tuple[str, bool]:
    refiner = LLMQueryRefiner(context)
    refined_query, changes, refined_entities, invalid_entities, can_be_answered = refiner.refine_query(
        user_query)

    if not can_be_answered:
        print_warning(
            "The query cannot be answered under the current context.")
        print_info("Explanation: " + refiner.explanation)
        return refiner.explanation, False

    if changes:
        print("We've made the following adjustments to your query:")
        for change in changes:
            print(f"- {change}")

    if invalid_entities:
        print("\nWarning: The following terms were not found in the database:")
        for entity in invalid_entities:
            print(
                f"- '{entity['potential_value']}' in {entity['table']}.{entity['column']}")
        print(
            "The refined query will be used, but it may not produce the expected results.")

    if refined_entities:
        print("\nWe found the following matches in the database:")
        for entity in refined_entities:
            print(f"- In {entity['table']}.{entity['column']}:")
            if len(entity['matches']) == 1:
                print(f"  * Exact match: {entity['matches'][0]}")
            else:
                for match in entity['matches']:
                    print(f"  * {match}")

    print_info(f"\nRefined query: {refined_query}\n")

    return refined_query, True
