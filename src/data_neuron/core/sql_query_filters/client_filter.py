from typing import Dict, Optional, List
from .sql_parser import ClientFilterApplier


class ClientFilterApplierImplementation(ClientFilterApplier):
    def __init__(self, client_tables: Dict[str, str], schemas: List[str] = ['main'], case_sensitive: bool = False):
        self.client_tables = client_tables
        self.schemas = schemas
        self.case_sensitive = case_sensitive

    def apply_filter(self, table_info: Dict[str, Optional[str]], client_id: int) -> str:
        table_name = table_info['name']
        schema = table_info['schema']
        alias = table_info['alias']

        matching_table = self._find_matching_table(table_name, schema)
        if matching_table:
            client_id_column = self.client_tables[matching_table]
            table_reference = alias or table_name
            return f'{self._quote_identifier(table_reference)}.{self._quote_identifier(client_id_column)} = {client_id}'
        return ''

    def _find_matching_table(self, table_name: str, schema: Optional[str] = None) -> Optional[str]:
        possible_names = [
            f"{schema}.{table_name}" if schema else table_name,
            table_name,
        ] + [f"{s}.{table_name}" for s in self.schemas]

        for name in possible_names:
            if self._case_insensitive_get(self.client_tables, name) is not None:
                return name
        return None

    def _case_insensitive_get(self, dict_obj: Dict[str, str], key: str) -> Optional[str]:
        if self.case_sensitive:
            return dict_obj.get(key)
        return next((v for k, v in dict_obj.items() if k.lower() == key.lower()), None)

    def _quote_identifier(self, identifier: str) -> str:
        return f'"{identifier}"'
