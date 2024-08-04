import sqlparse
from typing import Union, Dict, List, Any, Tuple, Optional
from sqlparse.sql import IdentifierList, Identifier, Function
from sqlparse.tokens import Keyword, DML
from .context_loader import ContextLoader
from ..db_operations.factory import DatabaseFactory
from ..api.main import call_neuron_api
from ..prompts.sql_query_prompt import sql_query_prompt
from .query_refiner import QueryRefiner
from .sql_query_filter import SQLQueryFilter
from ..utils.print import print_info, print_prompt, print_warning, print_success, print_error, create_box


MAX_CHAT_HISTORY = 5
MAX_RESULT_RECORDS = 3


class DataNeuron:
    def __init__(self, db_config: Union[str, Dict], context: Union[str, Dict], log: bool = False):
        self.db_config = db_config
        self.context = context
        self.db = None
        self.query_refiner = None
        self.chat_history = []
        self.log = log
        self.filter = None
        self.current_client_id = None

    def initialize(self):
        """Initialize the database connection and load the context."""
        if isinstance(self.db_config, str):
            self.db = DatabaseFactory.get_database()
        else:
            self.db = DatabaseFactory.get_database(self.db_config)

        context_loader = None
        if isinstance(self.context, str):
            context_loader = ContextLoader(self.context)
            self.context = context_loader.load()
            self.query_refiner = QueryRefiner(
                self.context, self.db, context_loader)
            client_info = self.context.get("client_info", {})
            client_tables = client_info.get("tables", {})
            schemas = client_info.get("schemas", ["main"])
            self.filter = SQLQueryFilter(client_tables, schemas)
        elif self.context is None:
            self.context = {}

        if self.log:
            print_info("DataNeuron initialized with database and context.")

    def query(self, question: str) -> Dict[str, Any]:
        """Execute a natural language query and return the SQL and result."""
        if not self.context or not self.db:
            raise ValueError(
                "DataNeuron is not initialized. Call initialize() first.")

        if self.log:
            print_info(f"Received question: {question}")

        refined_query, changes, refined_entities, invalid_entities = self.query_refiner.refine_query(
            question)

        if self.log:
            print_info(f"Refined query: {refined_query}")

        if not refined_query:
            if self.log:
                print_warning(
                    "Unable to generate a valid SQL query for the given question.")
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

        sql_query, explanation, references = self._extract_sql_explanation_and_references(
            llm_response)

        if not sql_query:
            if self.log:
                print_warning(
                    "The language model was unable to generate a valid SQL query.")
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

        if self.log:
            print_success(f"Generated SQL query: {sql_query}")
            print_prompt(f"Explanation: {explanation}")
            print_info(f"References: {references}")

        if self.current_client_id:
            sql_query = self._apply_client_filter(sql_query)

        result, column_names = self.execute_query_with_column_names(sql_query)

        if self.log:
            print_info("Query execution completed. Displaying results:")
            self._print_formatted_result(result, column_names)

        return {
            'original_question': question,
            'refined_question': refined_query,
            'refinement_changes': changes,
            'refined_entities': refined_entities,
            'invalid_entities': invalid_entities,
            'sql': sql_query,
            'column_names': column_names,
            'result': result,
            'explanation': llm_response
        }

    def chat(self, message: str) -> Tuple[Optional[str], Any]:
        """Process a chat message, maintain chat history, and return a response."""
        if not self.context or not self.db:
            raise ValueError(
                "DataNeuron is not initialized. Call initialize() first.")

        if self.log:
            print_info(f"Received chat message: {message}")

        self.chat_history.append({"role": "user", "content": message})

        formatted_history = self._format_chat_history()
        refined_query, changes, refined_entities, invalid_entities = self.query_refiner.refine_query(
            message, formatted_history)

        if self.log:
            print_info(f"Refined query: {refined_query}")

        response = ""
        result = []
        if not refined_query:
            response = "I'm sorry, but I couldn't understand your query in the context of our conversation and the database structure."
            if self.log:
                print_warning(
                    "Unable to understand the query. Can you try asking questions related to your db")
            return None, response
        else:
            prompt = sql_query_prompt(
                refined_query, self.context, self.db.db_type)
            llm_response = call_neuron_api(prompt)

            sql_query, explanation, references = self._extract_sql_explanation_and_references(
                llm_response)

            if not sql_query:
                response = "I'm sorry, but I couldn't generate a valid SQL query for your question."
                if self.log:
                    print_warning(
                        "The language model was unable to generate a valid SQL query.")
                return None, response
            else:
                if self.current_client_id:
                    sql_query = self._apply_client_filter(sql_query)

                result, column_names = self.execute_query_with_column_names(
                    sql_query)
                result_str = str(result[:MAX_RESULT_RECORDS])
                response = f"Based on your question, I've generated the following SQL query: {sql_query}\n\nHere's a sample of the results: {result_str}"

                if self.log:
                    print_success(f"Generated SQL query: {sql_query}")
                    print_info(f"Explanation: {explanation}")
                    print_info(f"References: {references}")
                    print_success(
                        "Query execution completed. Displaying results:\n")
                    self._print_formatted_result(result, column_names)

        self.chat_history.append({"role": "user", "content": message})
        self.chat_history.append({"role": "assistant", "content": response})

        if len(self.chat_history) > MAX_CHAT_HISTORY * 2:
            self.chat_history = self.chat_history[-MAX_CHAT_HISTORY * 2:]

        return sql_query, {"data": result, "column_names": column_names}

    def execute_query(self, sql_query: str) -> Any:
        """Execute a SQL query and return the result."""
        if not self.db:
            raise ValueError(
                "DataNeuron is not initialized. Call initialize() first.")

        if self.current_client_id:
            sql_query = self._apply_client_filter(sql_query)

        try:
            result = self.db.execute_query(sql_query)
            if self.log:
                print_success(f"Query executed successfully: {sql_query}")
            return result
        except Exception as e:
            if self.log:
                print_error(f"Error executing query: {str(e)}")
            return f"Error executing query: {str(e)}"

    def execute_query_with_column_names(self, sql_query: str) -> Any:
        """Execute a SQL query and return the result."""
        if not self.db:
            raise ValueError(
                "DataNeuron is not initialized. Call initialize() first.")

        if self.current_client_id:
            sql_query = self._apply_client_filter(sql_query)
        try:
            result = self.db.execute_query_with_column_names(sql_query)
            return result
        except Exception as e:
            if self.log:
                print_error(f"Error executing query: {str(e)}")
            return f"Error executing query: {str(e)}"

    def client_filtered_query(self, sql_query: str) -> str:
        if self.current_client_id:
            return self._apply_client_filter(sql_query)
        else:
            raise ValueError("You need to set client_id")

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

    def set_context(self, context):
        if isinstance(context, str):
            context_loader = ContextLoader(context)
            self.context = context_loader.load()
        else:
            self.context = context

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

    def _extract_sql_explanation_and_references(self, llm_response: str) -> Tuple[Optional[str], str, Dict[str, List[str]]]:
        sql_start = llm_response.find('<sql>')
        sql_end = llm_response.find('</sql>')
        explanation_start = llm_response.find('<explanation>')
        explanation_end = llm_response.find('</explanation>')
        references_start = llm_response.find('<references>')
        references_end = llm_response.find('</references>')

        sql_query = None
        explanation = "No explanation provided."
        references = {}

        if sql_start != -1 and sql_end != -1:
            sql_query = llm_response[sql_start+5:sql_end].strip()

        if explanation_start != -1 and explanation_end != -1:
            explanation = llm_response[explanation_start +
                                       13:explanation_end].strip()

        if references_start != -1 and references_end != -1:
            references_text = llm_response[references_start +
                                           12:references_end].strip()
            for line in references_text.split('\n'):
                if ':' in line:
                    key, value = line.split(':', 1)
                    references[key.strip()] = [item.strip()
                                               for item in value.strip().split(',')]

        return sql_query, explanation, references

    def _print_formatted_result(self, result, column_names):
        from tabulate import tabulate
        if not result:
            print_info("No results found.")
            return
        if len(result) == 1 and len(result[0]) == 1:
            # Single value result
            value = result[0][0]
            box = create_box("Query Result", f"{column_names[0]}: {value}", "")
            print(box)
        else:
            # Multiple rows or columns
            table = tabulate(result, headers=column_names, tablefmt="grid")
            print(table)

    def set_client_context(self, client_id: Any):
        """Set the current client context for filtering queries."""
        self.current_client_id = client_id
        if self.log:
            print_info(f"Set client context to client ID: {client_id}")

    def _apply_client_filter(self, sql_query: str) -> str:
        if self.current_client_id and self.filter:
            return self.filter.apply_client_filter(sql_query, self.current_client_id)
        return sql_query
