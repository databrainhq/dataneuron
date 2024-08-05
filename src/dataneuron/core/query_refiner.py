from typing import Dict, Tuple, List
import json
from ..api.main import call_neuron_api
from ..prompts.query_refinement_prompt import query_refinement_prompt
from ..db_operations.database_helpers import DatabaseHelper


class QueryRefiner:
    def __init__(self, context: Dict, db, context_loader):
        self.context = context
        self.db = db
        self.context_loader = context_loader

    def update_context(self, new_context: Dict):
        self.context = new_context

    def get_sample_data(self) -> str:
        sample_data = self.context.get('sample_data', {})
        sample_data_str = "Sample Data:\n"
        for table_name, data in sample_data.items():
            sample_data_str += f"Table: {table_name}\n"
            for row in data[:3]:  # Limit to 3 rows per table
                sample_data_str += f"  {row}\n"
        return sample_data_str

    def refine_query(self, user_query: str, formatted_history: str = "") -> Tuple[str, List[str], List[Dict], List[Dict]]:
        formatted_context = self.context.get('formatted_context', '')
        sample_data = self.get_sample_data()

        prompt = query_refinement_prompt(
            formatted_context, sample_data, user_query, formatted_history)
        response = call_neuron_api(prompt)

        try:
            parsed_response = json.loads(response)
            refined_query = parsed_response.get('refined_query')
            can_be_answered = parsed_response.get('can_be_answered')
            changes = parsed_response.get('changes', [])
            entities = parsed_response.get('entities', [])
        except (json.JSONDecodeError, KeyError):
            print("Error: Invalid response from LLM.")
            return None, [], [], []

        if not can_be_answered:
            exp = parsed_response.get('explanation')
            print("Explantion", exp)
            return None, exp, [], []

        if entities:
            is_valid, refined_entities, invalid_entities = self.validate_and_refine_entities(
                entities)
            if refined_entities:
                refined_query = self.further_refine_query(
                    refined_query, refined_entities)
        else:
            is_valid, refined_entities, invalid_entities = True, [], []
        return refined_query, changes, refined_entities, invalid_entities

    def validate_and_refine_entities(self, entities: List[Dict]) -> Tuple[bool, List[Dict], List[Dict]]:
        refined_entities = []
        invalid_entities = []
        db_helper = DatabaseHelper(self.db.db_type, self.db)
        for entity in entities:
            query = db_helper.top_few_records(
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

    def further_refine_query(self, query: str, refined_entities: List[Dict]) -> str:
        for entity in refined_entities:
            matches_len = len(entity['matches'])
            if matches_len == 1:
                query = query.replace(
                    f"containing '{entity['original_value']}'",
                    f"equal to '{entity['matches'][0]}'"
                )
            elif matches_len > 1 and len(entity['matches']) < 10:
                match_list = "', '".join(entity['matches'])
                query = query.replace(
                    f"containing '{entity['original_value']}'",
                    f"in ('{match_list}')"
                )
        return query
