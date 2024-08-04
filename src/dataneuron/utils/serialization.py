import json


def convert_to_serializable(data, columns):
    return [dict(zip(columns, row)) for row in data]


def ensure_serializable(obj):
    try:
        json.dumps(obj)
        return obj
    except (TypeError, OverflowError):
        if isinstance(obj, dict):
            return {k: ensure_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple)):
            return [ensure_serializable(item) for item in obj]
        elif hasattr(obj, '__dict__'):
            return ensure_serializable(obj.__dict__)
        else:
            return str(obj)
