import re

def _cleanup_whitespace(query: str) -> str:
    # Split the query into lines
    lines = query.split('\n')
    cleaned_lines = []
    for line in lines:
        # Remove leading/trailing whitespace from each line
        line = line.strip()
        # Replace multiple spaces with a single space, but not in quoted strings
        line = re.sub(r'\s+(?=(?:[^\']*\'[^\']*\')*[^\']*$)', ' ', line)
        # Ensure single space after commas, but not in quoted strings
        line = re.sub(
            r'\s*,\s*(?=(?:[^\']*\'[^\']*\')*[^\']*$)', ', ', line)
        cleaned_lines.append(line)
    # Join the lines back together
    return '\n'.join(cleaned_lines)