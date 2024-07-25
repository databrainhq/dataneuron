class DatabaseError(Exception):
    """Base class for database-related exceptions."""
    pass


class ConfigurationError(DatabaseError):
    """Raised when there's an issue with the database configuration."""
    pass


class ConnectionError(DatabaseError):
    """Raised when there's an issue connecting to the database."""
    pass


class OperationError(DatabaseError):
    """Raised when a database operation fails."""
    pass
