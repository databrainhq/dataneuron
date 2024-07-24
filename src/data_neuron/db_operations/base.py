from abc import ABC, abstractmethod


class DatabaseOperations(ABC):
    @abstractmethod
    def execute_query(self, query: str) -> str:
        pass

    @abstractmethod
    def get_schema_info(self) -> str:
        pass
