import sys
from functools import wraps
from .exceptions import DatabaseError, ConfigurationError, ConnectionError, OperationError
from ..utils.print import print_error


def handle_database_errors(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ConfigurationError as e:
            print_error(f"Configuration error: {str(e)}")
        except ConnectionError as e:
            print_error(f"Connection error: {str(e)}")
        except OperationError as e:
            print_error(f"Operation error: {str(e)}")
        except DatabaseError as e:
            print_error(f"Database error: {str(e)}")
        except Exception as e:
            print_error(f"Unexpected error: {str(e)}")
        sys.exit(1)
    return wrapper
