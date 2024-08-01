import yaml
from typing import Union, Dict, List, Any, Tuple, Optional
from .context_loader import ContextLoader
from ..db_operations.factory import DatabaseFactory
from ..api.main import call_neuron_api
from ..prompts.sql_query_prompt import sql_query_prompt
from .query_refiner import QueryRefiner


MAX_CHAT_HISTORY = 5
MAX_RESULT_RECORDS = 3


class DataNeuron:
    def __init__(self, db_config: Union[str, Dict], context: Union[str, Dict]):
        self.db_config = db_config
        self.context = context
        self.db = None
        self.query_refiner = None
        self.chat_history = []

    def initialize(self):
        """Initialize the database connection and load the context."""
        if isinstance(self.db_config, str):
            # If db_config is a string, assume it's a path to the config file
            self.db = DatabaseFactory.get_database()
        else:
            # If db_config is a dictionary, use it directly
            self.db = DatabaseFactory.get_database(self.db_config)

        context_loader = None
        if isinstance(self.context, str):
            context_loader = ContextLoader(self.context)
            self.context = context_loader.load()
        elif self.context is None:
            # If context is None, create an empty context
            self.context = {}

        self.query_refiner = QueryRefiner(
            self.context, self.db, context_loader)

    def execute_query(self, sql_query: str) -> Any:
        """Execute a SQL query and return the result."""
        if not self.db:
            raise ValueError(
                "DataNeuron is not initialized. Call initialize() first.")

        try:
            result = self.db.execute_query(sql_query)
            return result
        except Exception as e:
            return f"Error executing query: {str(e)}"

    def query(self, question: str) -> Dict[str, Any]:
        """Execute a natural language query and return the SQL and result."""
        if not self.context or not self.db:
            raise ValueError(
                "DataNeuron is not initialized. Call initialize() first.")

        # Refine the query
        refined_query, changes, refined_entities, invalid_entities = self.query_refiner.refine_query(
            question)

        if not refined_query:
            return {
                'original_question': question,
                'refined_question': None,
                'refinement_changes': [],
                'refined_entities': [],
                'invalid_entities': [],
                'sql': None,
                'result': None,
                'explanation': "Unable to generate a valid SQL query for the given question."
            }

        prompt = sql_query_prompt(refined_query, self.context, self.db.db_type)
        llm_response = call_neuron_api(prompt)

        # Extract SQL from LLM response (assuming it's wrapped in <sql> tags)
        sql_start = llm_response.find('<sql>')
        sql_end = llm_response.find('</sql>')

        if sql_start == -1 or sql_end == -1:
            return {
                'original_question': question,
                'refined_question': refined_query,
                'refinement_changes': changes,
                'refined_entities': refined_entities,
                'invalid_entities': invalid_entities,
                'sql': None,
                'result': None,
                'explanation': "The language model was unable to generate a valid SQL query."
            }

        sql_query = llm_response[sql_start+5:sql_end].strip()

        result = self.execute_query(sql_query)
        return {
            'original_question': question,
            'refined_question': refined_query,
            'refinement_changes': changes,
            'refined_entities': refined_entities,
            'invalid_entities': invalid_entities,
            'sql': sql_query,
            'result': result,
            'explanation': llm_response  # Include full LLM response for explanation
        }

    def chat(self, message: str) -> Tuple[Optional[str], Any]:
        """Process a chat message, maintain chat history, and return a response."""
        if not self.context or not self.db:
            raise ValueError(
                "DataNeuron is not initialized. Call initialize() first.")

        self.chat_history.append({"role": "user", "content": message})

        # Prepare the prompt with chat history and context
        formatted_history = self._format_chat_history()
        refined_query, changes, refined_entities, invalid_entities = self.query_refiner.refine_query(
            message, formatted_history)

        response = ""
        result = []
        if not refined_query:
            response = "I'm sorry, but I couldn't understand your query in the context of our conversation and the database structure."
        else:
            prompt = sql_query_prompt(
                refined_query, self.context, self.db.db_type)
            llm_response = call_neuron_api(prompt)

            sql_start = llm_response.find('<sql>')
            sql_end = llm_response.find('</sql>')

            if sql_start == -1 or sql_end == -1:
                response = "query can't be answered"

            sql_query = llm_response[sql_start+5:sql_end].strip()
            result = self.execute_query(sql_query)
            result_str = str(result[:MAX_RESULT_RECORDS])
            response += f"SQL query generated: {sql_query}, Result sample: {result_str}\n"

        # Update chat history
        self.chat_history.append({"role": "user", "content": message})
        self.chat_history.append({"role": "assistant", "content": response})

        # Trim chat history if it gets too long
        if len(self.chat_history) > MAX_CHAT_HISTORY * 2:
            self.chat_history = self.chat_history[-MAX_CHAT_HISTORY * 2:]

        return sql_query, result

    def get_table_list(self) -> List[str]:
        """Return a list of tables in the database."""
        if not self.db:
            raise ValueError(
                "DataNeuron is not initialized. Call initialize() first.")
        return self.db.get_table_list()

    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Return information about a specific table."""
        if not self.db:
            raise ValueError(
                "DataNeuron is not initialized. Call initialize() first.")
        return self.db.get_table_info(table_name)

    def _format_chat_history(self) -> str:
        formatted_history = ""
        # Get last MAX_CHAT_HISTORY exchanges
        for msg in self.chat_history[-MAX_CHAT_HISTORY * 2:]:
            if msg['role'] == 'user':
                formatted_history += f"User: {msg['content']}\n"
            else:
                formatted_history += f"Assistant: {msg['content']}\n"
            formatted_history += "\n"
        return formatted_history

    def set_context(self, context_name: str):
        context_loader = ContextLoader(context_name)
        self.context = context_loader.load()
        if self.query_refiner:
            self.query_refiner.update_context(self.context)
        else:
            self.query_refiner = QueryRefiner(
                self.context, self.db, context_loader)

    def set_chat_history(self, messages: List[Dict[str, str]]):
        self.chat_history = [
            {"role": msg["role"], "content": msg["content"]}
            for msg in messages
            if msg["role"] in ["user", "assistant"]
        ]
